// MARK: - Cache Middleware

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "Cache")

/// 缓存策略
public enum CachePolicy: Sendable {
    /// 不缓存
    case none
    /// 内存缓存指定时长
    case memory(ttl: TimeInterval)
}

/// 内存缓存中间件
///
/// 在 `didReceive` 阶段自动缓存 GET 请求的响应数据。
/// 外部通过 `cachedData(for:)` 查询缓存来跳过网络请求。
///
/// ```swift
/// let cacheMiddleware = CacheMiddleware(defaultTTL: 60)
///
/// // 在 send 之前手动检查缓存
/// if let data = cacheMiddleware.cachedData(for: url) {
///     return try decoder.decode(T.self, from: data)
/// }
///
/// // Endpoint 上声明缓存策略
/// Endpoint<Feed>(path: "/feed", cachePolicy: .memory(ttl: 30))
/// ```
/// - Note: @unchecked Sendable — NSCache 由 Apple 文档保证线程安全，pendingPolicies 由 OSAllocatedUnfairLock 保护
public final class CacheMiddleware: Middleware, @unchecked Sendable {

    private let defaultTTL: TimeInterval
    private let cache = NSCache<NSString, CacheEntry>()

    /// 当前请求的缓存策略查询回调（由 NetworkClient 注入）
    /// 保存 context.path → CachePolicy 映射，用于 didReceive 判断是否缓存
    private let pendingPolicies = OSAllocatedUnfairLock(initialState: [String: CachePolicy]())

    public init(defaultTTL: TimeInterval = 60, maxEntries: Int = 100) {
        self.defaultTTL = defaultTTL
        cache.countLimit = maxEntries
    }

    /// 响应拦截：自动缓存 GET 请求结果
    public func didReceive(data: Data, response: URLResponse, context: RequestContext) async throws -> Data {
        // 只缓存 GET + 200 的响应
        guard let httpResponse = response as? HTTPURLResponse,
              httpResponse.statusCode == 200,
              let url = httpResponse.url,
              (httpResponse.allHeaderFields["Content-Type"] as? String)?.contains("json") == true ||
              url.absoluteString.contains(context.path)
        else {
            return data
        }

        // 获取此请求注册的缓存策略
        let policy: CachePolicy? = pendingPolicies.withLock { policies in
            policies.removeValue(forKey: context.id)
        }

        let ttl: TimeInterval?
        switch policy {
        case .memory(let t):
            ttl = t
        case .some(.none), nil:
            return data
        }

        let key = Self.cacheKey(for: url)
        let entry = CacheEntry(data: data, expiresAt: Date().addingTimeInterval(ttl ?? defaultTTL))
        cache.setObject(entry, forKey: key as NSString)
        logger.debug("Cache stored: \(context.path) (TTL=\(ttl ?? self.defaultTTL)s)")

        return data
    }

    // MARK: - Public API

    /// 注册请求的缓存策略（由 NetworkClient 在发送前调用）
    public func registerPolicy(_ policy: CachePolicy, for contextID: String) {
        guard case .memory = policy else { return }
        pendingPolicies.withLock { $0[contextID] = policy }
    }

    /// 请求无论成功、失败还是取消都由 NetworkClient defer 调用；成功路径可重复移除，保持幂等。
    public func unregisterPolicy(for contextID: String) {
        _ = pendingPolicies.withLock { $0.removeValue(forKey: contextID) }
    }

    /// 只读诊断数量，供回归测试确认失败/取消和 dedup follower 没有泄漏策略状态。
    public var pendingPolicyCount: Int {
        pendingPolicies.withLock { $0.count }
    }

    /// 查询缓存
    public func cachedData(for url: URL) -> Data? {
        cachedData(for: Self.cacheKey(for: url))
    }

    /// 查询缓存（key）
    public func cachedData(for key: String) -> Data? {
        let nsKey = key as NSString
        guard let entry = cache.object(forKey: nsKey) else { return nil }
        guard Date() < entry.expiresAt else {
            cache.removeObject(forKey: nsKey)
            logger.debug("Cache expired: \(key)")
            return nil
        }
        logger.debug("Cache hit: \(key)")
        return entry.data
    }

    /// 手动存入缓存
    public func store(data: Data, for key: String, ttl: TimeInterval? = nil) {
        let entry = CacheEntry(data: data, expiresAt: Date().addingTimeInterval(ttl ?? defaultTTL))
        cache.setObject(entry, forKey: key as NSString)
    }

    /// 移除指定缓存
    public func invalidate(key: String) {
        cache.removeObject(forKey: key as NSString)
    }

    /// 清空所有缓存
    public func invalidateAll() {
        cache.removeAllObjects()
        logger.info("Cache cleared")
    }

    /// 生成缓存 key
    public static func cacheKey(for url: URL) -> String {
        url.absoluteString
    }
}

// MARK: - Cache Entry

private final class CacheEntry: NSObject {
    let data: Data
    let expiresAt: Date

    init(data: Data, expiresAt: Date) {
        self.data = data
        self.expiresAt = expiresAt
    }
}
