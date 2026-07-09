// MARK: - Rate Limit Middleware

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "RateLimit")

/// 令牌桶限流中间件
///
/// 在请求发出前检查令牌桶，超出速率时等待直到有可用令牌。
/// 适用于对第三方 API 的速率控制和防止被服务端限流。
///
/// ```swift
/// // 每秒最多 10 个请求
/// let limiter = RateLimitMiddleware(tokensPerSecond: 10, maxBurst: 15)
/// ```
public struct RateLimitMiddleware: Middleware {

    private let bucket: TokenBucket

    /// - Parameters:
    ///   - tokensPerSecond: 每秒补充令牌数
    ///   - maxBurst: 令牌桶最大容量（允许的突发请求数）
    public init(tokensPerSecond: Double, maxBurst: Int) {
        self.bucket = TokenBucket(
            tokensPerSecond: tokensPerSecond,
            maxBurst: maxBurst
        )
    }

    public func adapt(_ request: URLRequest, context: RequestContext) async throws -> URLRequest {
        try await bucket.acquire()
        return request
    }
}

// MARK: - Token Bucket

/// 令牌桶算法
final class TokenBucket: Sendable {

    private let tokensPerSecond: Double
    private let maxBurst: Int
    private let state: OSAllocatedUnfairLock<BucketState>

    init(tokensPerSecond: Double, maxBurst: Int) {
        self.tokensPerSecond = tokensPerSecond
        self.maxBurst = maxBurst
        self.state = OSAllocatedUnfairLock(
            initialState: BucketState(tokens: Double(maxBurst), lastRefill: Date())
        )
    }

    /// 消耗一个令牌，如果不够则等待
    func acquire() async throws {
        // 关键：在锁内「预扣」令牌（允许扣成负数），据此计算等待时间。
        // 旧实现只算 deficit、不预扣，睡醒后才 -1：并发请求会读到相同 deficit、睡相同时长后
        // 同时放行，突发量远超 tokensPerSecond。预扣后，后到的请求 deficit 更大 → 等更久 → 自然错峰。
        let waitTime: TimeInterval = state.withLock { s in
            refill(&s)
            let deficit = 1.0 - s.tokens
            s.tokens -= 1  // 预扣（可为负），后续 refill 会随时间把欠账补回
            return deficit > 0 ? deficit / tokensPerSecond : 0
        }

        if waitTime > 0 {
            logger.debug("Rate limit: waiting \(String(format: "%.1f", waitTime * 1000))ms")
            try await Task.sleep(for: .seconds(waitTime))
            // 令牌已在锁内预扣，睡醒后不再重复扣减
        }
    }

    private func refill(_ s: inout BucketState) {
        let now = Date()
        let elapsed = now.timeIntervalSince(s.lastRefill)
        let newTokens = elapsed * tokensPerSecond
        s.tokens = min(s.tokens + newTokens, Double(maxBurst))
        s.lastRefill = now
    }
}

private struct BucketState: Sendable {
    var tokens: Double
    var lastRefill: Date
}
