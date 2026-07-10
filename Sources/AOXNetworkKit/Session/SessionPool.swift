// MARK: - Session Pool

import Alamofire
import Foundation

/// 会话池：App 内所有网络请求的唯一出口
///
/// SessionPool 是唯一创建和持有 URLSession/Alamofire.Session 的地方。
/// 消费者通过三层 API 声明意图，永远不自行创建 Session。
///
/// ## 三层 API
///
/// | 层级 | API | 适用场景 | 示例 |
/// |------|-----|---------|------|
/// | **Alamofire** | `session(for:)` | 常规 API、Upload、Download | NetworkClient、UploadClient |
/// | **Infrastructure** | `bareData(for:)` | 需要绕过中间件链的内部请求 | AuthMiddleware、WBISigner |
/// | **Delegate** | `makeDelegateSession(config:delegate:queue:)` | 需要 delegate 回调的流式场景 | VideoResourceLoader、WebSocketClient |
///
/// ## 设计原则
///
/// - **唯一工厂**：App 内禁止直接创建 `URLSession` 或 `Alamofire.Session`
/// - **配置统一**：同优先级的 Alamofire Session 和 Delegate Session 共享超时/QoS/缓存策略
/// - **Cookie 边界统一**：bare、delegate 与 Alamofire Session 使用同一 CookieStorage 策略
/// - **职责分离**：`RequestPriority` 只用于 Alamofire 请求路由；
///   `SessionConfig` 的 `.media`/`.websocket` 预设供 Delegate Session 使用
public final class SessionPool: Sendable {

    public static let shared = SessionPool()

    private let apiSession: Session
    private let liveSession: Session
    private let prefetchSession: Session
    private let uploadSession: Session
    private let downloadSession: Session

    /// 该会话池统一使用的 HTTP CookieStorage 策略。
    ///
    /// 这是 URLSession 传输配置，不持久化业务 Cookie；Cookie 的真实状态仍由上层账号模块管理。
    public let cookieStoragePolicy: HTTPCookieStoragePolicy

    /// 无 Interceptor 的 URLSession，用于基础设施请求（Auth 验证、WBI 密钥获取等）
    private let _bareSession: URLSession

    /// 创建会话池
    ///
    /// - Parameters:
    ///   - interceptor: Alamofire `RequestInterceptor`。推荐使用 `makeInterceptor()` 工厂方法构建，
    ///                  组合 Adapter + RetryPolicy。
    ///   - serverTrustManager: SSL 证书信任管理器
    ///   - eventMonitors: Alamofire 事件监听器（如 `NetworkEventMonitor`）
    ///   - cookieStoragePolicy: 所有会话统一使用的 CookieStorage 策略。默认 `.shared` 保持历史行为；
    ///                          账号 Cookie 由业务层显式注入时应使用 `.disabled`，避免 URLSession
    ///                          再从共享 CookieStorage 隐式附加或接收 Cookie。
    ///
    /// ```swift
    /// let interceptor = makeInterceptor(
    ///     adapters: [MiddlewareAdapter(headersMiddleware)],
    ///     retryPolicy: NetworkKitRetryPolicy(retryLimit: 3)
    /// )
    /// let pool = SessionPool(interceptor: interceptor)
    /// ```
    public init(
        apiConfig: SessionConfig = .api,
        liveConfig: SessionConfig = .live,
        prefetchConfig: SessionConfig = .prefetch,
        uploadConfig: SessionConfig = .upload,
        downloadConfig: SessionConfig = .download,
        interceptor: (any RequestInterceptor)? = nil,
        serverTrustManager: ServerTrustManager? = nil,
        eventMonitors: [any EventMonitor] = [],
        cookieStoragePolicy: HTTPCookieStoragePolicy = .shared
    ) {
        self.cookieStoragePolicy = cookieStoragePolicy
        self._bareSession = URLSession(configuration: Self.makeURLSessionConfiguration(
            from: apiConfig,
            cookieStoragePolicy: cookieStoragePolicy
        ))
        self.apiSession = Self.makeSession(
            config: apiConfig,
            cookieStoragePolicy: cookieStoragePolicy,
            interceptor: interceptor,
            serverTrustManager: serverTrustManager,
            eventMonitors: eventMonitors
        )
        self.liveSession = Self.makeSession(
            config: liveConfig,
            cookieStoragePolicy: cookieStoragePolicy,
            interceptor: interceptor,
            serverTrustManager: serverTrustManager,
            eventMonitors: eventMonitors
        )
        self.prefetchSession = Self.makeSession(
            config: prefetchConfig,
            cookieStoragePolicy: cookieStoragePolicy,
            interceptor: interceptor,
            serverTrustManager: serverTrustManager,
            eventMonitors: eventMonitors
        )
        self.uploadSession = Self.makeSession(
            config: uploadConfig,
            cookieStoragePolicy: cookieStoragePolicy,
            interceptor: interceptor,
            serverTrustManager: serverTrustManager,
            eventMonitors: eventMonitors
        )
        self.downloadSession = Self.makeSession(
            config: downloadConfig,
            cookieStoragePolicy: cookieStoragePolicy,
            interceptor: interceptor,
            serverTrustManager: serverTrustManager,
            eventMonitors: eventMonitors
        )
    }

    // MARK: - Layer 1: Alamofire Session（带完整中间件链）

    /// 获取带完整 Interceptor 链的 Alamofire Session
    ///
    /// 适用于常规 API 调用、上传、下载等 Alamofire 管理的请求。
    public func session(for priority: RequestPriority) -> Session {
        switch priority {
        case .standard: return apiSession
        case .realtime: return liveSession
        case .prefetch: return prefetchSession
        case .upload:   return uploadSession
        case .download: return downloadSession
        }
    }

    // MARK: - Layer 2: Infrastructure（无 Interceptor，避免中间件循环依赖）

    /// 发起无中间件的基础设施请求
    ///
    /// 共享 API Session 的超时/缓存/QoS 配置，但不经过任何 Adapter / RetryPolicy。
    /// 适用于 Auth 验证、WBI 密钥获取等需要绕过中间件链的场景。
    ///
    /// ```swift
    /// // AuthMiddleware 验证 Cookie
    /// let (data, _) = try await SessionPool.shared.bareData(for: request)
    ///
    /// // WBISigner 获取密钥
    /// let (data, _) = try await SessionPool.shared.bareData(for: navRequest)
    /// ```
    public func bareData(for request: URLRequest) async throws -> (Data, URLResponse) {
        try await _bareSession.data(for: request)
    }

    // MARK: - Layer 3: Delegate Session Factory（delegate-based 场景）

    /// 创建带委托的 URLSession
    ///
    /// 使用指定 `SessionConfig` 的超时/缓存/QoS 配置创建 URLSession，
    /// 由调用方持有 delegate 生命周期。
    ///
    /// 适用于需要 `URLSessionDelegate` 回调的流式场景：
    /// - `VideoResourceLoader` → `.media` 配置（AVPlayer 数据流加载）
    /// - `WebSocketClient` → `.websocket` 配置（长连接）
    ///
    /// ```swift
    /// // VideoResourceLoader
    /// let session = SessionPool.shared.makeDelegateSession(
    ///     config: .media, delegate: self, queue: opQueue
    /// )
    ///
    /// // WebSocketClient
    /// let session = SessionPool.shared.makeDelegateSession(config: .websocket)
    /// let wsTask = session.webSocketTask(with: request)
    /// ```
    public func makeDelegateSession(
        config: SessionConfig,
        delegate: (any URLSessionDelegate)? = nil,
        queue: OperationQueue? = nil
    ) -> URLSession {
        URLSession(
            configuration: Self.makeURLSessionConfiguration(
                from: config,
                cookieStoragePolicy: cookieStoragePolicy
            ),
            delegate: delegate,
            delegateQueue: queue
        )
    }

    // MARK: - Internal

    private static func makeURLSessionConfiguration(
        from config: SessionConfig,
        cookieStoragePolicy: HTTPCookieStoragePolicy
    ) -> URLSessionConfiguration {
        let urlConfig = URLSessionConfiguration.default
        urlConfig.timeoutIntervalForRequest = config.requestTimeout
        urlConfig.timeoutIntervalForResource = config.resourceTimeout
        urlConfig.httpMaximumConnectionsPerHost = config.maxConnections
        urlConfig.networkServiceType = config.serviceType
        urlConfig.waitsForConnectivity = config.waitsForConnectivity
        urlConfig.requestCachePolicy = config.cachePolicy
        urlConfig.allowsConstrainedNetworkAccess = config.allowsConstrainedNetworkAccess
        cookieStoragePolicy.apply(to: urlConfig)

        if config.cachePolicy == .reloadIgnoringLocalCacheData {
            urlConfig.urlCache = nil
        }

        return urlConfig
    }

    private static func makeSession(
        config: SessionConfig,
        cookieStoragePolicy: HTTPCookieStoragePolicy,
        interceptor: (any RequestInterceptor)? = nil,
        serverTrustManager: ServerTrustManager? = nil,
        eventMonitors: [any EventMonitor] = []
    ) -> Session {
        Session(
            configuration: makeURLSessionConfiguration(
                from: config,
                cookieStoragePolicy: cookieStoragePolicy
            ),
            interceptor: interceptor,
            serverTrustManager: serverTrustManager,
            eventMonitors: eventMonitors
        )
    }
}

// MARK: - HTTP Cookie Storage Policy

/// URLSession 自动 CookieStorage 的使用策略。
///
/// 该类型只控制传输层是否从 CookieStorage 自动读取、向 CookieStorage 自动写入 Cookie，
/// 不负责业务 Cookie 的持久化，也不会影响调用方显式设置的 `Cookie` 请求头。
public enum HTTPCookieStoragePolicy: Sendable {
    /// 使用进程共享的 `HTTPCookieStorage.shared`。
    ///
    /// 这是 `URLSessionConfiguration.default` 的历史行为，也是 SessionPool 的默认值，
    /// 用于保持现有 AOXNetworkKit 消费方兼容。
    case shared

    /// 完全禁用 URLSession 的自动 CookieStorage 行为。
    ///
    /// 适用于由账号模块集中管理 Cookie、网络层仅显式注入 Header 的应用，避免共享存储中的
    /// 旧 Cookie 或跨域 Cookie 被 URLSession 隐式附加。
    case disabled

    /// 使用调用方提供的隔离 CookieStorage，例如 App Group 专属存储。
    case custom(HTTPCookieStorage)

    fileprivate func apply(to configuration: URLSessionConfiguration) {
        switch self {
        case .shared:
            configuration.httpShouldSetCookies = true
            configuration.httpCookieStorage = .shared

        case .disabled:
            configuration.httpShouldSetCookies = false
            configuration.httpCookieStorage = nil

        case .custom(let storage):
            configuration.httpShouldSetCookies = true
            configuration.httpCookieStorage = storage
        }
    }
}

// MARK: - Session Configuration

/// Session 配置值对象
public struct SessionConfig: Sendable {
    public let requestTimeout: TimeInterval
    public let resourceTimeout: TimeInterval
    public let maxConnections: Int
    public let serviceType: URLRequest.NetworkServiceType
    public let waitsForConnectivity: Bool
    /// URL 缓存策略（Upload/Download/Live 应禁用缓存避免浪费内存）
    public let cachePolicy: URLRequest.CachePolicy
    /// 是否允许在「低数据模式」下发起请求（Prefetch 应设为 false 节省流量）
    public let allowsConstrainedNetworkAccess: Bool

    public init(
        requestTimeout: TimeInterval,
        resourceTimeout: TimeInterval,
        maxConnections: Int,
        serviceType: URLRequest.NetworkServiceType,
        waitsForConnectivity: Bool,
        cachePolicy: URLRequest.CachePolicy = .useProtocolCachePolicy,
        allowsConstrainedNetworkAccess: Bool = true
    ) {
        self.requestTimeout = requestTimeout
        self.resourceTimeout = resourceTimeout
        self.maxConnections = maxConnections
        self.serviceType = serviceType
        self.waitsForConnectivity = waitsForConnectivity
        self.cachePolicy = cachePolicy
        self.allowsConstrainedNetworkAccess = allowsConstrainedNetworkAccess
    }

    /// 常规 API: Feed、详情、搜索、用户信息
    ///
    /// - requestTimeout: 15s — API 通常 1-5s 响应，15s 兼顾弱网但不让用户空等
    /// - resourceTimeout: 60s — 足够完成分页加载等较大响应
    /// - maxConnections: 4 — HTTP/2 下多路复用，4 连接足以覆盖并发场景
    /// - cachePolicy: 遵循服务端 Cache-Control
    public static let api = SessionConfig(
        requestTimeout: 15,
        resourceTimeout: 60,
        maxConnections: 4,
        serviceType: .default,
        waitsForConnectivity: false,
        cachePolicy: .useProtocolCachePolicy
    )

    /// 实时: 播放地址获取、直播间信令、弹幕端点
    ///
    /// - requestTimeout: 10s — 快速失败，让上层立即展示错误或触发重试
    /// - serviceType: .responsiveData — 系统层面优先调度（.responsiveAV 用于 AV 流传输，不适合 HTTP API）
    /// - cachePolicy: 忽略缓存 — 实时数据缓存无意义且可能导致播放过期地址
    public static let live = SessionConfig(
        requestTimeout: 10,
        resourceTimeout: 30,
        maxConnections: 2,
        serviceType: .responsiveData,
        waitsForConnectivity: false,
        cachePolicy: .reloadIgnoringLocalCacheData
    )

    /// 后台预取: 预加载下一页 Feed、推荐列表
    ///
    /// - requestTimeout: 30s — 后台任务容忍较慢响应，但 60s 过于宽松会阻塞队列
    /// - resourceTimeout: 300s — 允许较大预取负载（封面图批量加载等）
    /// - waitsForConnectivity: true — 等待网络恢复再预取，不浪费电量
    /// - allowsConstrainedNetworkAccess: false — 遵循「低数据模式」，不消耗用户流量做预取
    /// - cachePolicy: 优先使用本地缓存 — 预取数据通常有较长的新鲜度
    public static let prefetch = SessionConfig(
        requestTimeout: 30,
        resourceTimeout: 300,
        maxConnections: 2,
        serviceType: .background,
        waitsForConnectivity: true,
        cachePolicy: .returnCacheDataElseLoad,
        allowsConstrainedNetworkAccess: false
    )

    /// 上传: 图片/视频上传独立通道
    ///
    /// - requestTimeout: 60s — 上传期间持续有数据流动不会触发此超时；60s 保护初始连接
    /// - resourceTimeout: 600s — 100MB @3Mbps ≈ 270s，600s 留足弱网余量
    /// - maxConnections: 2 — 限制上行带宽占用，避免影响用户浏览体验
    /// - cachePolicy: 忽略缓存 — 上传响应无缓存价值
    public static let upload = SessionConfig(
        requestTimeout: 60,
        resourceTimeout: 600,
        maxConnections: 2,
        serviceType: .default,
        waitsForConnectivity: false,
        cachePolicy: .reloadIgnoringLocalCacheData
    )

    /// 下载: 视频/资源断点续传
    ///
    /// - requestTimeout: 30s — 如果服务器 30s 无响应，应判定源不可用并快速失败
    /// - resourceTimeout: 1800s — 500MB @2Mbps ≈ 2000s；30 分钟覆盖多数场景，
    ///   超大文件依赖 DownloadClient 的断点续传（重新创建请求会重置此计时器）
    /// - waitsForConnectivity: true — 网络中断时保持等待，恢复后自动继续
    /// - cachePolicy: 忽略缓存 — 大文件直接写磁盘，URLCache 不适合
    public static let download = SessionConfig(
        requestTimeout: 30,
        resourceTimeout: 1800,
        maxConnections: 3,
        serviceType: .default,
        waitsForConnectivity: true,
        cachePolicy: .reloadIgnoringLocalCacheData
    )

    /// 媒体流加载: AVPlayer ResourceLoader 的后端 HTTP 请求
    ///
    /// - requestTimeout: 10s — CDN 响应通常 <1s，快速超时让 AVPlayer 重选 CDN 节点
    /// - resourceTimeout: 1800s — 单个视频可能持续播放数十分钟
    /// - maxConnections: 2 — AVPlayer 通常只有 1-2 个并发 Range 请求
    /// - serviceType: .responsiveAV — 此处是真正的 AV 流传输，系统层面优先调度
    /// - cachePolicy: 忽略缓存 — 视频流数据量大，走 URLCache 无意义
    public static let media = SessionConfig(
        requestTimeout: 10,
        resourceTimeout: 1800,
        maxConnections: 2,
        serviceType: .responsiveAV,
        waitsForConnectivity: false,
        cachePolicy: .reloadIgnoringLocalCacheData
    )

    /// WebSocket 长连接: 弹幕、直播信令
    ///
    /// - requestTimeout: 10s — 握手阶段应快速完成
    /// - resourceTimeout: 86400s — WebSocket 连接可能持续数小时
    /// - maxConnections: 2 — 通常同时只有 1-2 个 WS 连接（弹幕 + 直播信令）
    /// - waitsForConnectivity: true — 断网时保持 session 等待恢复，配合 WebSocketClient 的自动重连
    /// - cachePolicy: 忽略缓存 — 长连接无缓存需求
    public static let websocket = SessionConfig(
        requestTimeout: 10,
        resourceTimeout: 86400,
        maxConnections: 2,
        serviceType: .responsiveData,
        waitsForConnectivity: true,
        cachePolicy: .reloadIgnoringLocalCacheData
    )
}
