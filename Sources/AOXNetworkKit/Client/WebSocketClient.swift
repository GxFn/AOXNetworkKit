// MARK: - WebSocket Client

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "WebSocket")

/// Ensures a CheckedContinuation is resumed at most once.
/// URLSession may invoke sendPing completion more than once during teardown
/// (task cancel + session invalidate), which would crash with CheckedContinuation.
private final class ContinuationOnceGuard<T: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<T, any Error>?

    init(_ continuation: CheckedContinuation<T, any Error>) {
        self.continuation = continuation
    }

    func resume(returning value: T) {
        lock.lock()
        let cont = continuation
        continuation = nil
        lock.unlock()
        cont?.resume(returning: value)
    }

    func resume(throwing error: any Error) {
        lock.lock()
        let cont = continuation
        continuation = nil
        lock.unlock()
        cont?.resume(throwing: error)
    }
}

/// WebSocket 消息类型
public enum WebSocketMessage: Sendable {
    case text(String)
    case data(Data)
}

/// WebSocket 连接状态
public enum WebSocketState: Sendable, Equatable {
    case connecting
    case connected
    case disconnected(reason: String?)
}

/// WebSocket 客户端
///
/// 基于 `URLSessionWebSocketTask`，支持自动重连和 AsyncSequence 消息流。
/// 使用共享的 `messages` stream，所有观察者接收相同的消息。
///
/// ```swift
/// let ws = WebSocketClient(url: URL(string: "wss://live.example.com/room/123")!)
///
/// // 获取消息流（必须在 connect 之前）
/// let stream = ws.messages
///
/// try await ws.connect()
/// try await ws.send(.text("{\"action\":\"join\"}"))
///
/// for await message in stream {
///     switch message {
///     case .text(let json): handleJSON(json)
///     case .data(let bytes): handleBinary(bytes)
///     }
/// }
/// ```
public final actor WebSocketClient {

    private let url: URL
    private let headers: [String: String]
    private let autoReconnect: Bool
    private let maxReconnectAttempts: Int
    private let reconnectBaseDelay: TimeInterval
    private let sessionPool: SessionPool

    private var task: URLSessionWebSocketTask?
    private var session: URLSession?
    private var reconnectCount = 0
    private var intentionalDisconnect = false
    private var receiveLoopTask: Task<Void, Never>?

    /// 消息广播：支持多个监听者
    private var continuations: [UUID: AsyncStream<WebSocketMessage>.Continuation] = [:]

    private(set) public var state: WebSocketState = .disconnected(reason: nil)

    public init(
        url: URL,
        headers: [String: String] = [:],
        autoReconnect: Bool = true,
        maxReconnectAttempts: Int = 5,
        reconnectBaseDelay: TimeInterval = 1.0,
        sessionPool: SessionPool = .shared
    ) {
        self.url = url
        self.headers = headers
        self.autoReconnect = autoReconnect
        self.maxReconnectAttempts = maxReconnectAttempts
        self.reconnectBaseDelay = reconnectBaseDelay
        self.sessionPool = sessionPool
    }

    // MARK: - Connect / Disconnect

    /// 建立连接
    ///
    /// 不使用 sendPing 验证握手，因为 URLSessionWebSocketTask.sendPing 的回调
    /// 在 delegate 为 nil 或握手未完成时可能永远不触发，导致连接永久挂起。
    /// 改为乐观启动接收循环：handshake 成功 → receive() 正常返回数据；
    /// 失败 → receive() 抛错 → handleDisconnect 处理重连。
    public func connect() async throws {
        try await connect(resetReconnectCount: true)
    }

    private func connect(resetReconnectCount: Bool) async throws {
        intentionalDisconnect = false
        if resetReconnectCount {
            reconnectCount = 0
        }

        var request = URLRequest(url: url)
        for (key, value) in headers {
            request.setValue(value, forHTTPHeaderField: key)
        }

        let session = sessionPool.makeDelegateSession(config: .websocket)
        let wsTask = session.webSocketTask(with: request)

        self.session = session
        self.task = wsTask
        self.state = .connecting

        wsTask.resume()

        self.state = .connected
        logger.info("WebSocket connected: \(self.url.absoluteString)")

        startReceiveLoop()
    }

    /// 断开连接
    public func disconnect(reason: String? = nil) {
        intentionalDisconnect = true
        receiveLoopTask?.cancel()
        receiveLoopTask = nil
        task?.cancel(with: .normalClosure, reason: reason?.data(using: .utf8))
        cleanup(reason: reason ?? "intentional")
    }

    // MARK: - Send

    /// 发送消息
    public func send(_ message: WebSocketMessage) async throws {
        guard let task, state == .connected else {
            throw NetworkError.transport(
                underlying: URLError(.notConnectedToInternet),
                requestID: UUID().uuidString
            )
        }

        switch message {
        case .text(let string):
            try await task.send(.string(string))
        case .data(let data):
            try await task.send(.data(data))
        }
    }

    // MARK: - Message Stream

    /// 创建消息接收 AsyncStream（可多次调用，每个调用者获得独立的 stream）
    ///
    /// 作为 actor 方法调用，保证 continuation 在返回前同步注册，
    /// 避免使用非结构化 Task 异步注册时消息被广播到空 continuations 的竞态。
    public func messageStream() -> AsyncStream<WebSocketMessage> {
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(of: WebSocketMessage.self)
        continuations[id] = continuation
        continuation.onTermination = { [weak self] _ in
            Task { [weak self] in await self?.removeContinuation(id: id) }
        }
        return stream
    }

    private func removeContinuation(id: UUID) {
        continuations.removeValue(forKey: id)
    }

    // MARK: - Ping

    /// 发送 ping
    public func ping() async throws {
        guard let task, state == .connected else {
            logger.warning("WebSocket ping skipped: not connected, url=\(self.url.absoluteString)")
            throw NetworkError.transport(
                underlying: URLError(.notConnectedToInternet),
                requestID: UUID().uuidString
            )
        }

        try await withCheckedThrowingContinuation { (cont: CheckedContinuation<Void, any Error>) in
            let once = ContinuationOnceGuard(cont)
            task.sendPing { error in
                if let error {
                    once.resume(throwing: error)
                } else {
                    once.resume(returning: ())
                }
            }
        }
    }

    // MARK: - Internal

    private func startReceiveLoop() {
        guard let task else { return }

        receiveLoopTask = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    let message = try await task.receive()
                    guard let self else { break }
                    let wsMessage: WebSocketMessage
                    switch message {
                    case .string(let text):
                        wsMessage = .text(text)
                    case .data(let data):
                        wsMessage = .data(data)
                    @unknown default:
                        continue
                    }
                    await self.broadcast(wsMessage)
                } catch {
                    if !Task.isCancelled, let self {
                        await self.handleDisconnect(error: error)
                    }
                    break
                }
            }
        }
    }

    private func broadcast(_ message: WebSocketMessage) {
        for (_, continuation) in continuations {
            continuation.yield(message)
        }
    }

    private func handleDisconnect(error: any Error) {
        let reason = error.localizedDescription
        cleanup(reason: reason)

        guard !intentionalDisconnect, autoReconnect, reconnectCount < maxReconnectAttempts else {
            logger.info("WebSocket won't reconnect: intentional=\(self.intentionalDisconnect), count=\(self.reconnectCount)")
            // 终止所有 stream
            for (_, continuation) in continuations {
                continuation.finish()
            }
            continuations.removeAll()
            return
        }

        reconnectCount += 1
        let delay = reconnectBaseDelay * pow(2, Double(reconnectCount - 1))
        logger.info("WebSocket reconnecting (#\(self.reconnectCount)) in \(String(format: "%.1f", delay))s")

        Task {
            try? await Task.sleep(for: .seconds(delay))
            guard !self.intentionalDisconnect else { return }
            do {
                try await self.connect(resetReconnectCount: false)
            } catch {
                await self.handleDisconnect(error: error)
            }
        }
    }

    private func cleanup(reason: String) {
        state = .disconnected(reason: reason)
        receiveLoopTask?.cancel()
        receiveLoopTask = nil
        task = nil
        session?.invalidateAndCancel()
        session = nil
        logger.info("WebSocket disconnected: \(reason)")
    }

    deinit {
        task?.cancel(with: .goingAway, reason: nil)
    }
}
