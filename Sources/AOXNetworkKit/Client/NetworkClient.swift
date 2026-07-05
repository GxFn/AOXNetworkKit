// MARK: - Network Client

import Alamofire
import Foundation
import OSLog

private let logger = Logger(subsystem: "com.networkkit", category: "Network")

/// 网络客户端协议
///
/// 面向协议设计，Repository 持有 `any NetworkClientProtocol`。
/// 测试时可注入 Mock 实现。
public protocol NetworkClientProtocol: Sendable {
    func send<T: Decodable & Sendable>(_ endpoint: Endpoint<T>) async throws -> T
}

/// 网络客户端：请求发送 + 中间件编排
///
/// 整个网络层的 **唯一出口**。
///
/// ## 两层拦截架构
///
/// **Layer 1 — Alamofire Session 级别**（通过 `SessionPool` 的 `Interceptor` 配置）
/// - `adapt`: 无状态请求修改（DefaultHeaders 等不依赖 RequestContext 的中间件）
/// - `retry`: 传输层重试由 `NetworkKitRetryPolicy` 处理（超时/DNS/连接丢失等）
///
/// **Layer 2 — NetworkClient Pipeline 级别**（通过 `middlewares` 参数配置）
/// - `adapt`: 有状态请求修改（Signing、RateLimit 等需要 RequestContext 的中间件）
/// - `didReceive`: 响应后处理（缓存等）
/// - `recover`: 业务级恢复（token 刷新等，仅在 Alamofire 重试不适用时触发）
///
/// > **注意**: 需要 `RequestContext` 的中间件（如 SigningMiddleware）必须放在 Pipeline 级别。
/// > Session 级别的 `MiddlewareAdapter` 会创建临时 Context，缺少 `requiresSigning` 等元数据。
///
/// ```
/// send() → cache check → CB preCheck → dedup
///     ↓
/// execute() → adapt pipeline → Alamofire Session → didReceive pipeline → decode
///                                                        ↑ business recover
/// ```
public struct NetworkClient: NetworkClientProtocol {

    /// 默认 baseURL（Endpoint 未指定 baseURL 时使用）
    public let defaultBaseURL: String

    private let sessionPool: SessionPool
    private let middlewares: [any Middleware]
    private let decoder: ResponseDecoder
    private let circuitBreaker: CircuitBreaker?
    private let deduplicator: RequestDeduplicator?

    public init(
        defaultBaseURL: String,
        sessionPool: SessionPool = .shared,
        middlewares: [any Middleware] = [],
        decoder: ResponseDecoder = .default,
        circuitBreaker: CircuitBreaker? = nil,
        deduplicator: RequestDeduplicator? = nil
    ) {
        self.defaultBaseURL = defaultBaseURL
        self.sessionPool = sessionPool
        self.middlewares = middlewares
        self.decoder = decoder
        self.circuitBreaker = circuitBreaker
        self.deduplicator = deduplicator
    }

    // MARK: - Send

    public func send<T: Decodable & Sendable>(_ endpoint: Endpoint<T>) async throws -> T {
        // 1. 内存缓存命中 → 跳过网络请求
        if let cached: T = checkCache(for: endpoint) {
            return cached
        }

        // 2. 熔断器预检 → 快速失败
        try circuitBreaker?.preCheck()

        // 3. 注册缓存策略（didReceive 阶段使用）
        let context = RequestContext(endpoint: endpoint)
        registerCachePolicy(endpoint, context: context)

        // 4. 执行请求（支持去重）
        if let deduplicator {
            return try await deduplicator.deduplicate(
                key: Self.deduplicationKey(for: endpoint)
            ) {
                try await self.executeWithCircuitBreaker(endpoint, context: context)
            }
        }

        return try await executeWithCircuitBreaker(endpoint, context: context)
    }

    // MARK: - Circuit Breaker Wrapper

    private func executeWithCircuitBreaker<T: Decodable & Sendable>(
        _ endpoint: Endpoint<T>,
        context: RequestContext
    ) async throws -> T {
        do {
            let result = try await execute(endpoint, context: context)
            circuitBreaker?.recordSuccess()
            return result
        } catch {
            circuitBreaker?.recordFailure()
            throw error
        }
    }

    // MARK: - Core Pipeline

    private func execute<T: Decodable & Sendable>(
        _ endpoint: Endpoint<T>,
        context: RequestContext
    ) async throws -> T {
        try Task.checkCancellation()

        do {
            // 1. Endpoint → URLRequest（参数编码由 Endpoint.asURLRequest 统一处理）
            var urlRequest = try endpoint.asURLRequest(baseURL: defaultBaseURL)

            // 2. Pipeline adapt 中间件链（Signing、RateLimit、Log 等需要 Context 的中间件）
            for middleware in middlewares {
                urlRequest = try await middleware.adapt(urlRequest, context: context)
            }

            // 3. Alamofire 发送请求
            //    Session 级 Interceptor.adapt() 处理无状态修改（DefaultHeaders 等）
            //    Session 级 RetryPolicy 处理传输层重试
            let session = sessionPool.session(for: endpoint.priority)
            let (rawData, urlResponse) = try await performRequest(urlRequest, session: session, context: context)

            // 4. didReceive 中间件链（缓存、日志等响应后处理）
            var data = rawData
            for middleware in middlewares {
                data = try await middleware.didReceive(data: data, response: urlResponse, context: context)
            }

            // 5. 解码 + 业务校验
            let result = try decoder.decode(T.self, from: data, context: context)

            logger.debug("[\(context.id.prefix(8))] ✔ \(endpoint.path) (\(String(format: "%.0f", context.elapsed * 1000))ms)")
            return result

        } catch let error as NetworkError {
            return try await attemptBusinessRecovery(from: error, endpoint: endpoint, context: context)
        } catch is CancellationError {
            logger.debug("[\(context.id.prefix(8))] Request cancelled: \(endpoint.path)")
            throw CancellationError()
        } catch {
            let networkError = NetworkError.transport(underlying: error, requestID: context.id)
            return try await attemptBusinessRecovery(from: networkError, endpoint: endpoint, context: context)
        }
    }

    // MARK: - Business-Level Recovery

    /// 业务级恢复（仅处理 Alamofire 无法自动重试的业务错误）
    ///
    /// 传输层重试（超时/DNS/连接丢失等）已由 Alamofire `RetryPolicy` 在 Session 级处理。
    /// 此方法仅处理业务层恢复，如：
    /// - token 过期 → 刷新后重试
    /// - 业务限流 → 等待后重试
    private static let maxBusinessRetries = 3

    private func attemptBusinessRecovery<T: Decodable & Sendable>(
        from error: NetworkError,
        endpoint: Endpoint<T>,
        context: RequestContext
    ) async throws -> T {
        guard context.retryCount < Self.maxBusinessRetries else {
            logger.warning("[\(context.id.prefix(8))] Business recovery exhausted (\(Self.maxBusinessRetries) attempts)")
            throw error
        }

        for middleware in middlewares {
            if let action = try await middleware.recover(from: error, context: context) {
                switch action {
                case .retry(let delay):
                    try Task.checkCancellation()
                    context.incrementRetry()
                    try await Task.sleep(for: .seconds(delay))
                    return try await execute(endpoint, context: context)
                }
            }
        }
        throw error
    }

    // MARK: - Perform Request

    private func performRequest(
        _ request: URLRequest,
        session: Session,
        context: RequestContext
    ) async throws -> (Data, URLResponse) {
        // Alamofire 完整链路：
        // 1. Interceptor.adapt() → 注入 headers/signing（Session 级配置）
        // 2. URLSession 发送请求
        // 3. .validate() → 校验 HTTP 状态码
        // 4. 如果失败 → Interceptor.retry() → RetryPolicy 判断是否重试
        // 5. .serializingData() → 获取原始 Data
        let response = await session.request(request)
            .validate(statusCode: 200..<300)
            .serializingData()
            .response

        if let afError = response.error {
            throw NetworkError.from(
                afError: afError,
                response: response.response,
                data: response.data,
                requestID: context.id
            )
        }

        guard let data = response.data else {
            throw NetworkError.transport(
                underlying: URLError(.badServerResponse),
                requestID: context.id
            )
        }

        let urlResponse = response.response ?? URLResponse()
        return (data, urlResponse)
    }

    // MARK: - Cache Integration

    /// 从内存缓存查找已缓存的响应
    private func checkCache<T: Decodable & Sendable>(for endpoint: Endpoint<T>) -> T? {
        guard case .memory = endpoint.cachePolicy else { return nil }
        guard let cache = middlewares.lazy.compactMap({ $0 as? CacheMiddleware }).first else { return nil }
        let urlString = (endpoint.baseURL ?? defaultBaseURL) + endpoint.path
        guard let url = URL(string: urlString) else { return nil }
        guard let data = cache.cachedData(for: url) else { return nil }
        let context = RequestContext(endpoint: endpoint)
        return try? decoder.decode(T.self, from: data, context: context)
    }

    /// 向 CacheMiddleware 注册缓存策略（didReceive 阶段使用）
    private func registerCachePolicy<T>(_ endpoint: Endpoint<T>, context: RequestContext) {
        guard case .memory = endpoint.cachePolicy else { return }
        guard let cache = middlewares.lazy.compactMap({ $0 as? CacheMiddleware }).first else { return }
        cache.registerPolicy(endpoint.cachePolicy, for: context.id)
    }

    // MARK: - Deduplication

    /// 生成去重 key：method + path + 排序后的参数
    private static func deduplicationKey<T>(for endpoint: Endpoint<T>) -> String {
        var key = "\(endpoint.method.rawValue):\(endpoint.path)"
        if let params = endpoint.parameters {
            let sorted = params.keys.sorted().map { "\($0)=\(params[$0]!)" }
            key += "?" + sorted.joined(separator: "&")
        }
        return key
    }
}
