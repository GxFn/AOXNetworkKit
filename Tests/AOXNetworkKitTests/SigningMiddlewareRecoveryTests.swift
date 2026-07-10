import Foundation
import XCTest
@testable import AOXNetworkKit

final class SigningMiddlewareRecoveryTests: XCTestCase {
    func testRecoverableSignerRetriesSignedRequestOnlyOnce() async throws {
        let signer = RecoverableSignerSpy(shouldRetry: true)
        let middleware = SigningMiddleware(signer: signer)
        let context = RequestContext(endpoint: Endpoint<TestPayload>(
            path: "/signed",
            requiresSigning: true
        ))
        let error = NetworkError.serverBusiness(
            code: -352,
            message: "signature rejected",
            requestID: context.id
        )

        let first = try await middleware.recover(from: error, context: context)
        guard case let .retry(delay)? = first else {
            return XCTFail("签名器确认可恢复时应请求一次重签")
        }
        XCTAssertEqual(delay, 0)
        let firstPrepareCount = await signer.prepareCount
        XCTAssertEqual(firstPrepareCount, 1)

        context.incrementRetry()
        let secondRecovery = try await middleware.recover(from: error, context: context)
        let secondPrepareCount = await signer.prepareCount
        XCTAssertNil(secondRecovery)
        XCTAssertEqual(secondPrepareCount, 1, "业务重试后不得再次刷新签名")
    }

    func testUnsignedRequestAndUnrecoverableErrorDoNotRetry() async throws {
        let signer = RecoverableSignerSpy(shouldRetry: false)
        let middleware = SigningMiddleware(signer: signer)
        let signedContext = RequestContext(endpoint: Endpoint<TestPayload>(
            path: "/signed",
            requiresSigning: true
        ))
        let unsignedContext = RequestContext(endpoint: Endpoint<TestPayload>(path: "/plain"))
        let error = NetworkError.serverBusiness(
            code: -412,
            message: "rate limited",
            requestID: signedContext.id
        )

        let signedRecovery = try await middleware.recover(from: error, context: signedContext)
        let unsignedRecovery = try await middleware.recover(from: error, context: unsignedContext)
        let prepareCount = await signer.prepareCount
        XCTAssertNil(signedRecovery)
        XCTAssertNil(unsignedRecovery)
        XCTAssertEqual(prepareCount, 1, "未签名请求不能调用签名恢复器")
    }
}

private struct TestPayload: Codable, Sendable {}

private actor RecoverableSignerSpy: RecoverableRequestSigner {
    let shouldRetry: Bool
    private(set) var prepareCount = 0

    init(shouldRetry: Bool) {
        self.shouldRetry = shouldRetry
    }

    func sign(url: URL, parameters: [String: any Sendable]) async throws -> URL { url }

    func prepareRetry(after error: NetworkError) async -> Bool {
        prepareCount += 1
        return shouldRetry
    }
}
