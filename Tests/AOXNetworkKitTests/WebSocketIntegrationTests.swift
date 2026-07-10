import Foundation
import XCTest
@testable import AOXNetworkKit

final class WebSocketIntegrationTests: XCTestCase {
    func testRealRFC6455HandshakeAndBidirectionalTextMessage() async throws {
        let fixture = try await makeFixture(path: "/integration/messages")
        let messageStream = await fixture.client.messageStream()
        let messageProbe = AsyncStreamProbe(messageStream)

        try await fixture.client.connect()
        let accepted = try await nextAcceptedConnection(from: fixture.serverEvents)
        XCTAssertEqual(accepted.path, "/integration/messages")
        let connectedState = await fixture.client.state
        XCTAssertEqual(connectedState, .connected)

        try await fixture.server.sendText("server-to-client", to: accepted.connectionID)
        let incoming = try await messageProbe.nextValue(label: "server text message")
        assertText(incoming, equals: "server-to-client")

        try await fixture.client.send(.text("client-to-server"))
        let outgoing = try await nextTextReceived(from: fixture.serverEvents)
        XCTAssertEqual(outgoing.connectionID, accepted.connectionID)
        XCTAssertEqual(outgoing.text, "client-to-server")

        await fixture.client.disconnect(reason: "integration test completed")
        await fixture.server.stop()
    }

    func testHTTP403UpgradeResponseFailsHandshakeWithoutExternalNetwork() async throws {
        let fixture = try await makeFixture(
            path: "/integration/forbidden",
            policy: .rejectForbidden
        )
        let stream = await fixture.client.messageStream()
        let streamProbe = AsyncStreamProbe(stream)

        do {
            try await fixture.client.connect()
            XCTFail("HTTP 403 不得被当成 WebSocket 握手成功")
        } catch let error as NetworkError {
            guard case .transport = error else {
                return XCTFail("403 握手拒绝应归入 transport，实际为：\(error)")
            }
        } catch {
            XCTFail("收到非预期错误类型：\(error)")
        }

        let rejection = try await nextRejectedHandshake(from: fixture.serverEvents)
        XCTAssertEqual(rejection.statusCode, 403)
        let rejectedState = await fixture.client.state
        guard case .disconnected(let reason) = rejectedState else {
            return XCTFail(
                "403 后客户端必须进入 disconnected，实际为：\(rejectedState)"
            )
        }
        XCTAssertNotNil(reason, "握手拒绝应保留可诊断的断开原因")
        try await streamProbe.waitUntilFinished(label: "403 terminal stream")

        await fixture.server.stop()
    }

    func testRemoteClosePreservesStructuredCodeAndReason() async throws {
        let fixture = try await makeFixture(path: "/integration/close")
        let stream = await fixture.client.messageStream()
        let streamProbe = AsyncStreamProbe(stream)

        try await fixture.client.connect()
        let accepted = try await nextAcceptedConnection(from: fixture.serverEvents)
        try await fixture.server.sendClose(
            code: URLSessionWebSocketTask.CloseCode.policyViolation.rawValue,
            reason: "session expired",
            to: accepted.connectionID
        )

        try await streamProbe.waitUntilFinished(label: "remote close stream")
        let remoteClose = await fixture.client.lastRemoteClose
        XCTAssertEqual(
            remoteClose,
            WebSocketCloseInfo(
                code: URLSessionWebSocketTask.CloseCode.policyViolation.rawValue,
                reason: "session expired"
            )
        )

        await fixture.server.stop()
    }

    func testAutomaticReconnectStopsAtConfiguredBudget() async throws {
        let fixture = try await makeFixture(
            path: "/integration/reconnect",
            autoReconnect: true,
            maxReconnectAttempts: 2,
            reconnectBaseDelay: 0
        )
        let stream = await fixture.client.messageStream()
        let streamProbe = AsyncStreamProbe(stream)

        try await fixture.client.connect()
        let first = try await nextAcceptedConnection(from: fixture.serverEvents)
        try await fixture.server.sendClose(
            code: URLSessionWebSocketTask.CloseCode.internalServerError.rawValue,
            reason: "retry-1",
            to: first.connectionID
        )

        let second = try await nextAcceptedConnection(from: fixture.serverEvents)
        try await fixture.server.sendClose(
            code: URLSessionWebSocketTask.CloseCode.internalServerError.rawValue,
            reason: "retry-2",
            to: second.connectionID
        )

        let third = try await nextAcceptedConnection(from: fixture.serverEvents)
        try await fixture.server.sendClose(
            code: URLSessionWebSocketTask.CloseCode.internalServerError.rawValue,
            reason: "retry-3",
            to: third.connectionID
        )

        try await streamProbe.waitUntilFinished(label: "reconnect budget exhaustion")
        let acceptedHandshakeCount = await fixture.server.acceptedHandshakeCount
        XCTAssertEqual(acceptedHandshakeCount, 3, "初次连接 + 两次重连后必须停止")
        let finalRemoteClose = await fixture.client.lastRemoteClose
        XCTAssertEqual(
            finalRemoteClose,
            WebSocketCloseInfo(
                code: URLSessionWebSocketTask.CloseCode.internalServerError.rawValue,
                reason: "retry-3"
            )
        )

        await fixture.server.stop()
    }

    func testSlowSubscriberKeepsOnlyNewestMessagesWithinBoundedBuffer() async throws {
        let fixture = try await makeFixture(
            path: "/integration/buffer",
            messageBufferLimit: 2
        )
        let slowStream = await fixture.client.messageStream()
        let fastStream = await fixture.client.messageStream()
        let fastProbe = AsyncStreamProbe(fastStream)

        try await fixture.client.connect()
        let accepted = try await nextAcceptedConnection(from: fixture.serverEvents)

        for index in 0..<5 {
            let text = "message-\(index)"
            try await fixture.server.sendText(text, to: accepted.connectionID)
            assertText(
                try await fastProbe.nextValue(label: "fast subscriber message \(index)"),
                equals: text
            )
        }

        // send() 与 broadcast() 都在 WebSocketClient actor 上；服务端收到该 barrier 时，
        // 最后一条入站消息一定已经向所有订阅者完成 yield。
        try await fixture.client.send(.text("buffer-barrier"))
        let barrier = try await nextTextReceived(from: fixture.serverEvents)
        XCTAssertEqual(barrier.text, "buffer-barrier")

        let slowProbe = AsyncStreamProbe(slowStream)
        assertText(
            try await slowProbe.nextValue(label: "slow subscriber penultimate message"),
            equals: "message-3"
        )
        assertText(
            try await slowProbe.nextValue(label: "slow subscriber newest message"),
            equals: "message-4"
        )

        await fixture.client.disconnect(reason: "bounded buffer test completed")
        await fixture.server.stop()
    }

    func testWSSDoesNotDowngradeWhenLocalPeerRejectsTLSClientHello() async throws {
        let fixture = try await makeFixture(
            path: "/integration/tls",
            scheme: "wss",
            policy: .rejectTLSClientHello
        )

        do {
            try await fixture.client.connect()
            XCTFail("wss 不得在 TLS 被拒绝后降级为明文 WebSocket")
        } catch let error as NetworkError {
            guard case .transport = error else {
                return XCTFail("TLS 拒绝应归入 transport，实际为：\(error)")
            }
        } catch {
            XCTFail("收到非预期错误类型：\(error)")
        }

        let event = try await fixture.serverEvents.nextValue(label: "TLS ClientHello rejection")
        guard case .tlsClientHelloRejected(_, let hasHandshakeRecord) = event else {
            return XCTFail("收到非预期服务端事件：\(event)")
        }
        XCTAssertTrue(
            hasHandshakeRecord,
            "本地服务应观察到 TLS Handshake record，不能是明文 GET"
        )

        await fixture.server.stop()
    }

    // MARK: - Fixtures

    private struct Fixture: Sendable {
        let server: LocalRFC6455Server
        let serverEvents: AsyncStreamProbe<LocalRFC6455Server.Event>
        let client: WebSocketClient
    }

    private func makeFixture(
        path: String,
        scheme: String = "ws",
        policy: LocalRFC6455Server.HandshakePolicy = .accept,
        autoReconnect: Bool = false,
        maxReconnectAttempts: Int = 0,
        reconnectBaseDelay: TimeInterval = 0,
        messageBufferLimit: Int = 8
    ) async throws -> Fixture {
        let server = try LocalRFC6455Server(policy: policy)
        let port = try await server.start()
        let eventProbe = AsyncStreamProbe(await server.events())
        let url = try XCTUnwrap(URL(string: "\(scheme)://127.0.0.1:\(port)\(path)"))
        let client = WebSocketClient(
            url: url,
            autoReconnect: autoReconnect,
            maxReconnectAttempts: maxReconnectAttempts,
            reconnectBaseDelay: reconnectBaseDelay,
            sessionPool: SessionPool(cookieStoragePolicy: .disabled),
            handshakeTimeout: 3,
            pingTimeout: 3,
            messageBufferLimit: messageBufferLimit
        )

        addTeardownBlock {
            await client.disconnect(reason: "integration test teardown")
            await server.stop()
        }
        return Fixture(server: server, serverEvents: eventProbe, client: client)
    }

    private func nextAcceptedConnection(
        from probe: AsyncStreamProbe<LocalRFC6455Server.Event>
    ) async throws -> (connectionID: Int, path: String) {
        for _ in 0..<8 {
            let event = try await probe.nextValue(label: "accepted WebSocket handshake")
            if case .handshakeAccepted(let connectionID, let path) = event {
                return (connectionID, path)
            }
        }
        throw WebSocketIntegrationTestError.unexpectedEvent("未收到 101 握手事件")
    }

    private func nextRejectedHandshake(
        from probe: AsyncStreamProbe<LocalRFC6455Server.Event>
    ) async throws -> (connectionID: Int, statusCode: Int) {
        for _ in 0..<4 {
            let event = try await probe.nextValue(label: "rejected WebSocket handshake")
            if case .handshakeRejected(let connectionID, let statusCode) = event {
                return (connectionID, statusCode)
            }
        }
        throw WebSocketIntegrationTestError.unexpectedEvent("未收到 HTTP Upgrade 拒绝事件")
    }

    private func nextTextReceived(
        from probe: AsyncStreamProbe<LocalRFC6455Server.Event>
    ) async throws -> (connectionID: Int, text: String) {
        for _ in 0..<8 {
            let event = try await probe.nextValue(label: "client WebSocket text frame")
            if case .textReceived(let connectionID, let text) = event {
                return (connectionID, text)
            }
        }
        throw WebSocketIntegrationTestError.unexpectedEvent("未收到客户端文本帧")
    }

    private func assertText(
        _ message: WebSocketMessage,
        equals expected: String,
        file: StaticString = #filePath,
        line: UInt = #line
    ) {
        guard case .text(let actual) = message else {
            return XCTFail("预期文本消息，实际为二进制消息", file: file, line: line)
        }
        XCTAssertEqual(actual, expected, file: file, line: line)
    }
}

private enum WebSocketIntegrationTestError: Error, Sendable, Equatable {
    case deadlineExceeded(String)
    case streamFinished(String)
    case unexpectedEvent(String)
}

/// 将 AsyncStream 持续抽取到可取消 inbox，测试代码再用 deadline 等待事件。
/// 这样既不会因为 iterator 的 inout 生命周期跨 Task 触发数据竞争，
/// 也不会无限挂起测试。
private final class AsyncStreamProbe<Element: Sendable>: Sendable {
    private enum ProbeEvent: Sendable {
        case value(Element)
        case finished
    }

    private let inbox: AsyncDeadlineInbox<ProbeEvent>
    private let collectionTask: Task<Void, Never>

    init(_ stream: AsyncStream<Element>) {
        let inbox = AsyncDeadlineInbox<ProbeEvent>()
        self.inbox = inbox
        collectionTask = Task {
            for await value in stream {
                await inbox.send(.value(value))
            }
            await inbox.send(.finished)
        }
    }

    func nextValue(
        timeout: Duration = .seconds(3),
        label: String
    ) async throws -> Element {
        switch try await inbox.next(timeout: timeout, label: label) {
        case .value(let value):
            return value
        case .finished:
            throw WebSocketIntegrationTestError.streamFinished(label)
        }
    }

    func waitUntilFinished(
        timeout: Duration = .seconds(3),
        label: String
    ) async throws {
        while true {
            switch try await inbox.next(timeout: timeout, label: label) {
            case .value:
                continue
            case .finished:
                return
            }
        }
    }

    deinit {
        collectionTask.cancel()
    }
}

/// 多事件门闩：生产回调与测试等待者通过 actor 串行化，
/// deadline 负责失败收口。
private actor AsyncDeadlineInbox<Element: Sendable> {
    private var buffered: [Element] = []
    private var waiterOrder: [UUID] = []
    private var waiters: [UUID: CheckedContinuation<Element, any Error>] = [:]

    func send(_ value: Element) {
        while let waiterID = waiterOrder.first {
            waiterOrder.removeFirst()
            guard let continuation = waiters.removeValue(forKey: waiterID) else { continue }
            continuation.resume(returning: value)
            return
        }
        buffered.append(value)
    }

    func next(timeout: Duration, label: String) async throws -> Element {
        try await withThrowingTaskGroup(of: Element.self) { group in
            group.addTask {
                try await self.nextValue()
            }
            group.addTask {
                try await Task.sleep(for: timeout)
                throw WebSocketIntegrationTestError.deadlineExceeded(label)
            }
            defer { group.cancelAll() }
            guard let value = try await group.next() else {
                throw WebSocketIntegrationTestError.deadlineExceeded(label)
            }
            return value
        }
    }

    private func nextValue() async throws -> Element {
        if !buffered.isEmpty {
            return buffered.removeFirst()
        }

        let waiterID = UUID()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                if Task.isCancelled {
                    continuation.resume(throwing: CancellationError())
                } else {
                    waiterOrder.append(waiterID)
                    waiters[waiterID] = continuation
                }
            }
        } onCancel: {
            Task {
                await self.cancel(waiterID: waiterID)
            }
        }
    }

    private func cancel(waiterID: UUID) {
        waiterOrder.removeAll { $0 == waiterID }
        waiters.removeValue(forKey: waiterID)?.resume(throwing: CancellationError())
    }
}
