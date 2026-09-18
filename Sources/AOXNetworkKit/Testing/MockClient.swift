// MARK: - Mock Client

import Foundation
import os

/// 预置响应条目
///
/// - Note: @unchecked Sendable — `data: (any Sendable)?` 存在类型无法被编译器自动推导
public struct MockResponse: @unchecked Sendable {
    let data: (any Sendable)?
    public let delay: TimeInterval
    public let error: NetworkError?

    /// 成功响应
    public static func success<T: Sendable>(_ value: T, delay: TimeInterval = 0) -> MockResponse {
        MockResponse(data: value, delay: delay, error: nil)
    }

    /// 失败响应
    public static func failure(_ error: NetworkError, delay: TimeInterval = 0) -> MockResponse {
        MockResponse(data: nil, delay: delay, error: error)
    }
}

/// 请求历史记录
public struct RecordedRequest: Sendable {
    public let path: String
    public let method: HTTPMethod
    public let parameters: [String: String]?
    public let timestamp: Date
}

/// 测试用 Mock 网络客户端
///
/// 遵循 `NetworkClientProtocol`，可注入 Repository 进行单元测试。
///
/// ```swift
/// let mock = MockClient()
/// mock.stub("popular") { .success(PopularResponse(list: mockVideos)) }
///
/// let repo = HomeRepository(client: mock)
/// let videos = try await repo.fetchPopular(page: 1)
///
/// XCTAssertEqual(mock.requests.count, 1)
/// XCTAssertEqual(mock.requests[0].path, "/x/web-interface/popular")
/// ```
/// - Note: @unchecked Sendable — class 类型，所有可变状态由 OSAllocatedUnfairLock<State> 保护
public final class MockClient: NetworkClientProtocol, @unchecked Sendable {

    private struct State {
        var stubs: [(pattern: String, factory: @Sendable (String) -> MockResponse)] = []
        var fallback: MockResponse?
        var requests: [RecordedRequest] = []
    }

    private let state = OSAllocatedUnfairLock(initialState: State())

    /// 请求历史（线程安全只读副本）
    public var requests: [RecordedRequest] {
        state.withLock { $0.requests }
    }

    /// 兜底响应（无匹配 stub 时使用）
    public var fallback: MockResponse? {
        get { state.withLock { $0.fallback } }
        set { state.withLock { $0.fallback = newValue } }
    }

    public init() {}

    // MARK: - Stub

    /// 注册 stub：路径包含 pattern 时返回指定响应
    public func stub(_ pattern: String, response: @Sendable @escaping (String) -> MockResponse) {
        state.withLock { $0.stubs.append((pattern: pattern, factory: response)) }
    }

    /// 注册固定 stub
    public func stub(_ pattern: String, returning response: MockResponse) {
        stub(pattern) { _ in response }
    }

    /// 清除所有 stub 和请求记录
    public func reset() {
        state.withLock {
            $0.stubs.removeAll()
            $0.requests.removeAll()
            $0.fallback = nil
        }
    }

    // MARK: - NetworkClientProtocol

    public func send<T: Decodable & Sendable>(_ endpoint: Endpoint<T>) async throws -> T {
        let record = RecordedRequest(
            path: endpoint.path,
            method: endpoint.method,
            parameters: endpoint.parameters?.mapValues { "\($0)" },
            timestamp: Date()
        )

        // 锁内原子记录并选择规则，锁外执行用户 factory。factory 可能读取 requests 或
        // 更新 fallback；在非递归 unfair lock 内调用会重入同一把锁并触发运行时异常。
        let selection: (factory: (@Sendable (String) -> MockResponse)?, fallback: MockResponse?) = state.withLock { s in
            s.requests.append(record)
            let factory = s.stubs.first { endpoint.path.contains($0.pattern) }?.factory
            return (factory, s.fallback)
        }
        let mockResponse = selection.factory?(endpoint.path) ?? selection.fallback

        guard let mockResponse else {
            throw NetworkError.invalidURL("No stub for path: \(endpoint.path)")
        }

        if mockResponse.delay > 0 {
            try await Task.sleep(for: .seconds(mockResponse.delay))
        }

        if let error = mockResponse.error {
            throw error
        }

        guard let value = mockResponse.data as? T else {
            throw NetworkError.decoding(
                underlying: DecodingError.typeMismatch(
                    T.self,
                    .init(codingPath: [], debugDescription: "MockClient: expected \(T.self), got \(type(of: mockResponse.data))")
                ),
                rawData: nil,
                requestID: UUID().uuidString
            )
        }

        return value
    }
}
