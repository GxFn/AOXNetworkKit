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
        let waitTime: TimeInterval = state.withLock { s in
            refill(&s)
            if s.tokens >= 1 {
                s.tokens -= 1
                return 0
            }
            // 需要等待的时间
            let deficit = 1.0 - s.tokens
            return deficit / tokensPerSecond
        }

        if waitTime > 0 {
            logger.debug("Rate limit: waiting \(String(format: "%.1f", waitTime * 1000))ms")
            try await Task.sleep(for: .seconds(waitTime))
            // 等待后扣减令牌
            state.withLock { s in
                refill(&s)
                s.tokens = max(s.tokens - 1, 0)
            }
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
