// MARK: - Request Signer Protocol

import Foundation

/// 请求签名协议
///
/// 网络层只定义签名的抽象接口，不关心具体签名算法（WBI、HMAC、OAuth 等）。
/// 业务方自行实现并注入到 `SigningMiddleware`。
public protocol RequestSigner: Sendable {
    /// 对请求 URL 和参数进行签名，返回签名后的完整 URL
    func sign(url: URL, parameters: [String: any Sendable]) async throws -> URL
}

/// 可在服务端明确拒绝签名后刷新临时密钥的签名器。
///
/// NetworkKit 只负责“一次、无延迟”的业务恢复编排；是否属于密钥失效由具体签名协议判断，
/// 避免把风控、限流或普通 4xx 误重试成签名刷新。
public protocol RecoverableRequestSigner: RequestSigner {
    func prepareRetry(after error: NetworkError) async -> Bool
}
