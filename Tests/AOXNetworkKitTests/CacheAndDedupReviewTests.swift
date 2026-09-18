import Foundation
import Testing
@testable import AOXNetworkKit

@Suite
struct CacheAndDedupReviewTests {
    @Test
    func adaptedResponseUsesOriginalRequestCacheIdentity() async throws {
        let cache = CacheMiddleware()
        let original = try #require(URL(string: "https://example.test/fixture?page=1"))
        let signed = try #require(URL(string: "https://example.test/fixture?page=1&signature=fixture"))
        let context = RequestContext(id: "cache-review", path: "/fixture")
        let body = Data("{\"value\":42}".utf8)
        cache.registerPolicy(.memory(ttl: 30), for: context.id, cacheKey: CacheMiddleware.cacheKey(for: original))
        let response = try #require(HTTPURLResponse(
            url: signed, statusCode: 200, httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"]
        ))
        _ = try await cache.didReceive(data: body, response: response, context: context)
        #expect(cache.cachedData(for: original) == body)
        #expect(cache.cachedData(for: signed) == nil)
        #expect(cache.pendingPolicyCount == 0)

        // 缓存读取使用真实 NetworkClient.send；miss 会在 adapt 明确失败，绝不访问外网。
        let client = NetworkClient(
            defaultBaseURL: "https://example.test",
            middlewares: [MustUseCacheMiddleware(), cache]
        )
        let payload: CacheDedupPayload = try await client.send(Endpoint(
            path: "/fixture", parameters: ["page": 1], requiresSigning: true,
            cachePolicy: .memory(ttl: 30)
        ))
        #expect(payload.value == 42)
        let different = try #require(URL(string: "https://example.test/fixture?page=2"))
        #expect(cache.cachedData(for: different) == nil)
    }

    @Test
    func cancellingOneDedupWaiterFinishesItWithoutCancellingSharedWork() async throws {
        let dedup = RequestDeduplicator()
        let started = WebSocketAsyncGate<Void>()
        let finishWork = WebSocketAsyncGate<Int>()
        let cancelledResult = WebSocketAsyncGate<Bool>()
        let counter = ReviewWorkCounter()
        let first = Task {
            try await dedup.deduplicate(key: "fixture") {
                await counter.increment()
                started.resolve(.success(()))
                return try await finishWork.wait()
            }
        }
        try await started.wait()
        let follower = Task {
            try await dedup.deduplicate(key: "fixture") {
                await counter.increment()
                return -1
            }
        }
        let observer = Task {
            do {
                _ = try await follower.value
                cancelledResult.resolve(.success(false))
            } catch is CancellationError {
                cancelledResult.resolve(.success(true))
            } catch {
                cancelledResult.resolve(.failure(error))
            }
        }
        defer {
            finishWork.resolve(.success(42))
            first.cancel()
            follower.cancel()
            observer.cancel()
        }
        follower.cancel()
        let timeout = Task {
            do { try await Task.sleep(for: .seconds(2)) } catch { return }
            cancelledResult.resolve(.failure(ReviewTimeout()))
        }
        defer { timeout.cancel() }

        #expect(try await cancelledResult.wait())
        #expect(await counter.count == 1)
        finishWork.resolve(.success(42))
        #expect(try await first.value == 42)

        // 真实 work 完成后 key 已回收，新的同 key 请求必须开始新 work。
        let next = try await dedup.deduplicate(key: "fixture") {
            await counter.increment()
            return 7
        }
        #expect(next == 7)
        #expect(await counter.count == 2)
    }

    @Test
    func failedWorkReleasesItsKeyForTheNextRequest() async throws {
        let dedup = RequestDeduplicator()
        do {
            let _: Int = try await dedup.deduplicate(key: "failure") { throw ReviewTimeout() }
            Issue.record("夹具 work 必须失败")
        } catch is ReviewTimeout {
            // expected
        }
        let value = try await dedup.deduplicate(key: "failure") { 9 }
        #expect(value == 9)
    }
}

private struct CacheDedupPayload: Codable, Sendable { let value: Int }
private struct ReviewTimeout: Error {}
private struct MustUseCacheMiddleware: Middleware {
    func adapt(_ request: URLRequest, context: RequestContext) async throws -> URLRequest {
        throw NetworkError.invalidURL("缓存未命中；回归夹具禁止发网")
    }
}
private actor ReviewWorkCounter {
    private(set) var count = 0
    func increment() { count += 1 }
}
