import Foundation
import Testing
@testable import AOXNetworkKit

@Suite
struct CircuitAndMockReviewTests {
    @Test(arguments: NonAvailabilityFailure.allCases)
    func halfOpenProbeIsReleasedForEveryNeutralExit(_ failure: NonAvailabilityFailure) async throws {
        let breaker = CircuitBreaker(failureThreshold: 1, resetTimeout: 0)
        breaker.recordFailure()
        let client = NetworkClient(
            defaultBaseURL: "https://example.test",
            middlewares: [FailingAdaptMiddleware(failure: failure)],
            circuitBreaker: breaker
        )
        do {
            let _: ReviewPayload = try await client.send(Endpoint(path: "/fixture"))
            Issue.record("夹具必须在实际发送前抛出指定错误")
        } catch {
            // 这里的中间件在发网前失败；只检查许可结算，不使用真实网络。
        }
        let nextProbe = try breaker.beginRequest()
        breaker.recordSuccess(for: nextProbe)
        #expect(breaker.currentState == .closed)
    }

    @Test
    func staleNormalRequestCannotReleaseOrCloseCurrentProbe() throws {
        let breaker = CircuitBreaker(failureThreshold: 1, resetTimeout: 0)
        let oldRequest = try breaker.beginRequest()
        breaker.recordFailure()
        let currentProbe = try breaker.beginRequest()

        breaker.recordIgnored(for: oldRequest)
        breaker.recordSuccess(for: oldRequest)
        #expect(throws: NetworkError.self) { try breaker.preCheck() }
        breaker.recordIgnored(for: currentProbe)
        let replacementProbe = try breaker.beginRequest()
        breaker.recordSuccess(for: replacementProbe)
        #expect(breaker.currentState == .closed)
    }

    @Test
    func malformedDedupKeyDoesNotConsumeHalfOpenProbe() async throws {
        let breaker = CircuitBreaker(failureThreshold: 1, resetTimeout: 0)
        breaker.recordFailure()
        let client = NetworkClient(
            defaultBaseURL: "https://[",
            circuitBreaker: breaker,
            deduplicator: RequestDeduplicator()
        )
        do {
            let _: ReviewPayload = try await client.send(Endpoint(path: ""))
            Issue.record("非法 URL 必须构建失败")
        } catch {
            // endpoint 构造失败不得领取/遗失半开许可。
        }
        let probe = try breaker.beginRequest()
        breaker.recordSuccess(for: probe)
        #expect(breaker.currentState == .closed)
    }

    @Test
    func stubFactoryCanReadHistoryAndUpdateFallback() async throws {
        let mock = MockClient()
        mock.stub("/fixture") { _ in
            let count = mock.requests.count
            mock.fallback = .success(ReviewPayload(value: 99))
            return .success(ReviewPayload(value: count))
        }

        let first: ReviewPayload = try await mock.send(Endpoint(path: "/fixture"))
        let second: ReviewPayload = try await mock.send(Endpoint(path: "/fallback"))
        #expect(first.value == 1)
        #expect(second.value == 99)
        #expect(mock.requests.count == 2)
    }

    @Test
    func concurrentStubFactoriesKeepAllRequestRecords() async throws {
        let mock = MockClient()
        mock.stub("/fixture") { _ in
            .success(ReviewPayload(value: mock.requests.count))
        }
        try await withThrowingTaskGroup(of: ReviewPayload.self) { group in
            for _ in 0..<16 {
                group.addTask { try await mock.send(Endpoint(path: "/fixture")) }
            }
            for try await payload in group {
                #expect((1...16).contains(payload.value))
            }
        }
        #expect(mock.requests.count == 16)
    }
}

enum NonAvailabilityFailure: CaseIterable, Sendable {
    case cancelled, notFound, business, decoding, invalidURL

    func error(context: RequestContext) -> any Error {
        switch self {
        case .cancelled: return CancellationError()
        case .notFound: return NetworkError.httpStatus(code: 404, data: nil, requestID: context.id)
        case .business: return NetworkError.serverBusiness(code: -101, message: "fixture", requestID: context.id)
        case .decoding: return NetworkError.decoding(underlying: URLError(.cannotDecodeContentData), rawData: nil, requestID: context.id)
        case .invalidURL: return NetworkError.invalidURL("fixture")
        }
    }
}

private struct FailingAdaptMiddleware: Middleware {
    let failure: NonAvailabilityFailure
    func adapt(_ request: URLRequest, context: RequestContext) async throws -> URLRequest {
        throw failure.error(context: context)
    }
}

private struct ReviewPayload: Codable, Sendable {
    let value: Int
}
