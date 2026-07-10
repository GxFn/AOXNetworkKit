import Alamofire
import Foundation
import XCTest
import os
@testable import AOXNetworkKit

final class DownloadTaskStateMachineTests: XCTestCase {
    override func setUp() {
        super.setUp()
        DownloadURLProtocolStub.reset()
    }

    override func tearDown() {
        DownloadURLProtocolStub.reset()
        super.tearDown()
    }

    func testConcurrentWaitersShareOneRequestAndTerminalResultIsReusable() async throws {
        let payload = Data("shared-result".utf8)
        DownloadURLProtocolStub.configure(payload: payload, delay: 0.08)
        let fixture = makeFixture()

        let first = Task { try await fixture.task.result }
        let second = Task { try await fixture.task.result }

        let firstResult = try await first.value
        let secondResult = try await second.value
        XCTAssertEqual(try Data(contentsOf: firstResult.fileURL), payload)
        XCTAssertEqual(firstResult.fileURL, secondResult.fileURL)
        XCTAssertEqual(firstResult.fileSize, Int64(payload.count))
        XCTAssertEqual(DownloadURLProtocolStub.requestCount, 1)

        let repeatedResult = try await fixture.task.result
        XCTAssertEqual(repeatedResult.fileURL, firstResult.fileURL)
        XCTAssertEqual(DownloadURLProtocolStub.requestCount, 1, "读取完成态不得重启下载")
    }

    func testCancellingOneWaiterDoesNotCancelSharedDownload() async throws {
        let payload = Data("other-waiter-must-survive".utf8)
        DownloadURLProtocolStub.configure(payload: payload, delay: 0.2)
        let fixture = makeFixture()

        let cancelledWaiter = Task { try await fixture.task.result }
        let survivingWaiter = Task { try await fixture.task.result }
        try await waitForRequestCount(1)
        cancelledWaiter.cancel()

        do {
            _ = try await cancelledWaiter.value
            XCTFail("被取消的等待者应立即收到 CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("收到非预期错误：\(error)")
        }

        let result = try await survivingWaiter.value
        XCTAssertEqual(try Data(contentsOf: result.fileURL), payload)
        XCTAssertEqual(DownloadURLProtocolStub.requestCount, 1)
    }

    func testExplicitCancelFinishesAllWaitersAndIsSticky() async throws {
        DownloadURLProtocolStub.configure(payload: Data("too-late".utf8), delay: 1)
        let fixture = makeFixture()

        let first = Task { try await fixture.task.result }
        let second = Task { try await fixture.task.result }
        try await waitForRequestCount(1)
        await fixture.task.cancel()

        await assertCancellation(first)
        await assertCancellation(second)

        do {
            _ = try await fixture.task.result
            XCTFail("取消终态再次读取仍应抛出 CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("收到非预期错误：\(error)")
        }
        XCTAssertEqual(DownloadURLProtocolStub.requestCount, 1)
    }

    func testFailureIsSharedAndRepeatedReadDoesNotRetryImplicitly() async throws {
        DownloadURLProtocolStub.configure(
            payload: Data("server-error".utf8),
            delay: 0.05,
            statusCode: 503
        )
        let fixture = makeFixture()

        let first = Task { try await fixture.task.result }
        let second = Task { try await fixture.task.result }
        await assertHTTPStatus(first, code: 503)
        await assertHTTPStatus(second, code: 503)

        do {
            _ = try await fixture.task.result
            XCTFail("失败终态再次读取仍应返回同一类错误")
        } catch let error as NetworkError {
            guard case .httpStatus(let code, _, _) = error else {
                return XCTFail("收到非预期 NetworkError：\(error)")
            }
            XCTAssertEqual(code, 503)
        } catch {
            XCTFail("收到非预期错误：\(error)")
        }
        XCTAssertEqual(DownloadURLProtocolStub.requestCount, 1, "读取失败终态不得隐式重试")
    }

    func testImmediateResumeWaitsForPauseCallbackAndStartsOneNewGeneration() async throws {
        let payload = Data("resumed-generation".utf8)
        DownloadURLProtocolStub.configure(payload: payload, delay: 0.2)
        let fixture = makeFixture()

        let waiter = Task { try await fixture.task.result }
        try await waitForRequestCount(1)
        await fixture.task.pause()
        await fixture.task.resume()

        let result = try await waiter.value
        XCTAssertEqual(try Data(contentsOf: result.fileURL), payload)
        XCTAssertEqual(
            DownloadURLProtocolStub.requestCount,
            2,
            "resume 应等待旧 generation 的暂停回调，再且仅再创建一次请求"
        )
    }

    // MARK: - Fixture

    private func makeFixture() -> (task: AOXNetworkKit.DownloadTask, destination: URL) {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [DownloadURLProtocolStub.self]
        let session = Session(configuration: configuration)
        let client = DownloadClient(session: session)
        let destination = FileManager.default.temporaryDirectory
            .appending(path: "aoxnetworkkit-download-tests")
            .appending(path: UUID().uuidString)
            .appendingPathExtension("bin")
        addTeardownBlock {
            try? FileManager.default.removeItem(at: destination)
        }
        let url = URL(string: "https://download.unit.test/media.bin")!
        return (client.download(url: url, to: destination), destination)
    }

    private func waitForRequestCount(
        _ expectedCount: Int,
        timeout: Duration = .seconds(2)
    ) async throws {
        let clock = ContinuousClock()
        let deadline = clock.now.advanced(by: timeout)
        while DownloadURLProtocolStub.requestCount < expectedCount {
            guard clock.now < deadline else {
                XCTFail("等待请求启动超时，expected=\(expectedCount)，actual=\(DownloadURLProtocolStub.requestCount)")
                throw TimeoutError()
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    private func assertCancellation(
        _ task: Task<DownloadResult, any Error>,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            _ = try await task.value
            XCTFail("等待者应收到 CancellationError", file: file, line: line)
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("收到非预期错误：\(error)", file: file, line: line)
        }
    }

    private func assertHTTPStatus(
        _ task: Task<DownloadResult, any Error>,
        code expectedCode: Int,
        file: StaticString = #filePath,
        line: UInt = #line
    ) async {
        do {
            _ = try await task.value
            XCTFail("下载应返回 HTTP 状态错误", file: file, line: line)
        } catch let error as NetworkError {
            guard case .httpStatus(let code, _, _) = error else {
                return XCTFail("收到非预期 NetworkError：\(error)", file: file, line: line)
            }
            XCTAssertEqual(code, expectedCode, file: file, line: line)
        } catch {
            XCTFail("收到非预期错误：\(error)", file: file, line: line)
        }
    }
}

private struct TimeoutError: Error {}

/// 使用 URLProtocol 在进程内构造下载响应，不依赖外网，也不暴露真实 URL/Cookie。
private final class DownloadURLProtocolStub: URLProtocol, @unchecked Sendable {
    private struct Configuration: Sendable {
        let payload: Data
        let delay: TimeInterval
        let statusCode: Int
    }

    private struct StubState: Sendable {
        var configuration = Configuration(payload: Data(), delay: 0, statusCode: 200)
        var requestCount = 0
    }

    private static let state = OSAllocatedUnfairLock<StubState>(initialState: StubState())

    private var responseWorkItem: DispatchWorkItem?

    static var requestCount: Int {
        state.withLock { $0.requestCount }
    }

    static func configure(payload: Data, delay: TimeInterval, statusCode: Int = 200) {
        state.withLock {
            $0.configuration = Configuration(
                payload: payload,
                delay: delay,
                statusCode: statusCode
            )
            $0.requestCount = 0
        }
    }

    static func reset() {
        configure(payload: Data(), delay: 0)
    }

    override class func canInit(with request: URLRequest) -> Bool {
        request.url?.host == "download.unit.test"
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        let snapshot = Self.state.withLock { state -> Configuration in
            state.requestCount += 1
            return state.configuration
        }
        let workItem = DispatchWorkItem { [weak self] in
            guard let self,
                  let responseWorkItem = self.responseWorkItem,
                  !responseWorkItem.isCancelled,
                  let url = self.request.url else {
                return
            }
            let response = HTTPURLResponse(
                url: url,
                statusCode: snapshot.statusCode,
                httpVersion: "HTTP/1.1",
                headerFields: [
                    "Content-Type": "application/octet-stream",
                    "Content-Length": String(snapshot.payload.count),
                ]
            )!
            self.client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            self.client?.urlProtocol(self, didLoad: snapshot.payload)
            self.client?.urlProtocolDidFinishLoading(self)
        }
        responseWorkItem = workItem
        DispatchQueue.global(qos: .userInitiated).asyncAfter(
            deadline: .now() + snapshot.delay,
            execute: workItem
        )
    }

    override func stopLoading() {
        responseWorkItem?.cancel()
        responseWorkItem = nil
    }
}
