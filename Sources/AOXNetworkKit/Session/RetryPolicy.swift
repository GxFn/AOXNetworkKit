// MARK: - Retry Middleware

import Alamofire
import Foundation
import OSLog

private let logger = Logger(subsystem: "com.networkkit", category: "Retry")

/// Alamofire 原生重试策略（推荐）
///
/// 基于 Alamofire `RetryPolicy`，内置指数退避 + HTTP 状态码/URLError 分类。
/// 额外集成 NetworkKit 的 `NetworkError.isTransient` 判断。
///
/// ```swift
/// let retryPolicy = NetworkKitRetryPolicy(retryLimit: 3)
///
/// // 注入 SessionPool (所有请求自动重试传输层错误)
/// let interceptor = makeInterceptor(retryPolicy: retryPolicy)
/// let pool = SessionPool(interceptor: interceptor)
/// ```
public final class NetworkKitRetryPolicy: RetryPolicy {

    /// 创建 NetworkKit 定制的重试策略
    ///
    /// 默认参数:
    /// - 重试上限: 3 次
    /// - 退避底数: 2 (delay = 0.5 * 2^retryCount)
    /// - 退避系数: 0.5
    /// - 可重试方法: GET, HEAD, OPTIONS, PUT, DELETE, TRACE
    /// - 可重试状态码: 408, 429, 500, 502, 503, 504
    /// - 可重试 URLError: 超时, DNS 失败, 连接中断等 20+ 种
    public init(
        retryLimit: UInt = 3,
        exponentialBackoffBase: UInt = RetryPolicy.defaultExponentialBackoffBase,
        exponentialBackoffScale: Double = 0.5
    ) {
        super.init(
            retryLimit: retryLimit,
            exponentialBackoffBase: exponentialBackoffBase,
            exponentialBackoffScale: exponentialBackoffScale,
            // 不把 POST 纳入可重试：Alamofire 默认因幂等性排除 POST，
            // 若重试「超时但服务端已成功」的写请求（点赞/投币/评论/发弹幕）会产生重复副作用。
            retryableHTTPMethods: RetryPolicy.defaultRetryableHTTPMethods,
            retryableHTTPStatusCodes: RetryPolicy.defaultRetryableHTTPStatusCodes.union([429]),
            retryableURLErrorCodes: RetryPolicy.defaultRetryableURLErrorCodes
        )
    }

    override public func shouldRetry(request: Request, dueTo error: any Error) -> Bool {
        // 先检查 NetworkError.isTransient
        if let networkError = error as? NetworkError {
            return networkError.isTransient
        }

        // 回退到 Alamofire 默认判断（HTTP 状态码 + URLError 分类）
        return super.shouldRetry(request: request, dueTo: error)
    }

    override public func retry(
        _ request: Request,
        for session: Session,
        dueTo error: any Error,
        completion: @escaping @Sendable (RetryResult) -> Void
    ) {
        super.retry(request, for: session, dueTo: error) { result in
            if case .retryWithDelay(let delay) = result {
                logger.info("Retry #\(request.retryCount + 1)/\(self.retryLimit): delay \(String(format: "%.1f", delay))s — \(request.request?.url?.path ?? "unknown")")
            }
            completion(result)
        }
    }
}
