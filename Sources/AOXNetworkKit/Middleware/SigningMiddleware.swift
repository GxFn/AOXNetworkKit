// MARK: - Signing Middleware

import Foundation

/// 通用请求签名中间件
///
/// 当 `Endpoint.requiresSigning == true` 时，使用注入的 `RequestSigner` 对请求签名。
/// 网络模块不知道具体签名算法（WBI / HMAC / OAuth），只调用 `signer.sign()`。
public struct SigningMiddleware: Middleware {

    private let signer: any RequestSigner

    public init(signer: any RequestSigner) {
        self.signer = signer
    }

    public func adapt(_ request: URLRequest, context: RequestContext) async throws -> URLRequest {
        guard context.requiresSigning else { return request }

        guard let originalURL = request.url,
              let components = URLComponents(url: originalURL, resolvingAgainstBaseURL: false) else {
            return request
        }

        // 提取现有 query 参数
        var params: [String: any Sendable] = [:]
        for item in components.queryItems ?? [] {
            if let value = item.value {
                params[item.name] = value
            }
        }

        let signedURL = try await signer.sign(url: originalURL, parameters: params)

        var mutableRequest = request
        mutableRequest.url = signedURL
        return mutableRequest
    }

    public func recover(from error: NetworkError, context: RequestContext) async throws -> RecoveryAction? {
        guard context.requiresSigning,
              context.retryCount == 0,
              let recoverableSigner = signer as? any RecoverableRequestSigner else {
            return nil
        }

        // 最多只允许签名器主动刷新一次。下一轮 execute 会重新经过 adapt，生成新签名；
        // 若仍被拒绝则直接把真实业务错误交给上层，不能形成风控请求放大。
        guard await recoverableSigner.prepareRetry(after: error) else { return nil }
        return .retry(after: 0)
    }
}
