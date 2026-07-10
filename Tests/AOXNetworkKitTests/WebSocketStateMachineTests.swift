import Foundation
import XCTest
@testable import AOXNetworkKit

final class WebSocketStateMachineTests: XCTestCase {
    func testGatePreservesHandshakeThatArrivesBeforeWaiter() async throws {
        let gate = WebSocketAsyncGate<Int>()

        XCTAssertTrue(gate.resolve(.success(42)))
        XCTAssertFalse(gate.resolve(.success(99)), "一次性门闩不得被晚到回调覆盖")

        let value = try await gate.wait()
        XCTAssertEqual(value, 42)
    }

    func testGateResumesWaiterOnlyOnceWhenCallbacksRace() async throws {
        let gate = WebSocketAsyncGate<String>()
        let waiter = Task { try await gate.wait() }

        await Task.yield()
        let results = await withTaskGroup(of: Bool.self, returning: [Bool].self) { group in
            for value in ["open", "close", "timeout"] {
                group.addTask {
                    gate.resolve(.success(value))
                }
            }

            var results: [Bool] = []
            for await result in group {
                results.append(result)
            }
            return results
        }

        _ = try await waiter.value
        XCTAssertEqual(results.filter { $0 }.count, 1, "竞争回调只能有一个恢复 continuation")
    }

    func testCancellingWaiterUnblocksGate() async {
        let gate = WebSocketAsyncGate<Void>()
        let waiter = Task { try await gate.wait() }

        waiter.cancel()

        do {
            try await waiter.value
            XCTFail("取消后应抛出 CancellationError")
        } catch is CancellationError {
            // expected
        } catch {
            XCTFail("收到非预期错误：\(error)")
        }
    }

    func testDelegateConfirmsHandshakeOnlyFromDidOpen() async throws {
        let gate = WebSocketAsyncGate<Void>()
        let recorder = TerminationRecorder()
        let delegate = WebSocketSessionDelegate(
            generation: 7,
            handshakeGate: gate
        ) { generation, error in
            recorder.record(generation: generation, error: error)
        }
        let task = URLSession.shared.webSocketTask(
            with: URL(string: "wss://unit-test.invalid/socket")!
        )

        let waiter = Task { try await gate.wait() }
        delegate.urlSession(
            .shared,
            webSocketTask: task,
            didOpenWithProtocol: nil
        )

        try await waiter.value
        XCTAssertEqual(recorder.count, 0, "握手成功不是终止事件")
        task.cancel(with: .normalClosure, reason: nil)
    }

    func testDelegateCoalescesCloseAndCompletionCallbacks() async {
        let gate = WebSocketAsyncGate<Void>()
        let recorder = TerminationRecorder()
        let delegate = WebSocketSessionDelegate(
            generation: 11,
            handshakeGate: gate
        ) { generation, error in
            recorder.record(generation: generation, error: error)
        }
        let task = URLSession.shared.webSocketTask(
            with: URL(string: "wss://unit-test.invalid/socket")!
        )

        delegate.urlSession(
            .shared,
            webSocketTask: task,
            didCloseWith: .goingAway,
            reason: Data("server restart".utf8)
        )
        delegate.urlSession(
            .shared,
            task: task,
            didCompleteWithError: URLError(.networkConnectionLost)
        )

        XCTAssertEqual(recorder.count, 1)
        XCTAssertEqual(recorder.lastGeneration, 11)

        do {
            try await gate.wait()
            XCTFail("握手前关闭必须让连接失败")
        } catch let error as WebSocketTransportError {
            XCTAssertEqual(
                error,
                .closed(code: URLSessionWebSocketTask.CloseCode.goingAway.rawValue, reason: "server restart")
            )
        } catch {
            XCTFail("收到非预期错误：\(error)")
        }
        task.cancel(with: .normalClosure, reason: nil)
    }
}

private final class TerminationRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var records: [(generation: UInt64, error: any Error)] = []

    var count: Int {
        lock.withLock { records.count }
    }

    var lastGeneration: UInt64? {
        lock.withLock { records.last?.generation }
    }

    func record(generation: UInt64, error: any Error) {
        lock.withLock {
            records.append((generation, error))
        }
    }
}
