// MARK: - WebSocket Client

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "WebSocket")

/// WebSocket 内部传输错误。
///
/// 保留明确的阶段和关闭信息，避免上层最终只看到无法定位的 `not connected`。
enum WebSocketTransportError: LocalizedError, Sendable, Equatable {
    case handshakeTimedOut(seconds: TimeInterval)
    case pingTimedOut(seconds: TimeInterval)
    case closed(code: Int, reason: String?)
    case transportCompleted
    case duplicateWaiter

    var errorDescription: String? {
        switch self {
        case .handshakeTimedOut(let seconds):
            return "WebSocket handshake timed out after \(String(format: "%.1f", seconds))s"
        case .pingTimedOut(let seconds):
            return "WebSocket ping timed out after \(String(format: "%.1f", seconds))s"
        case .closed(let code, let reason):
            let suffix = reason.map { ": \($0)" } ?? ""
            return "WebSocket closed (code=\(code))\(suffix)"
        case .transportCompleted:
            return "WebSocket transport completed"
        case .duplicateWaiter:
            return "WebSocket async gate only supports one waiter"
        }
    }

    var closeInfo: WebSocketCloseInfo? {
        guard case .closed(let code, let reason) = self else { return nil }
        return WebSocketCloseInfo(code: code, reason: reason)
    }
}

/// 可在回调早于 async 等待注册时保存结果的一次性门闩。
///
/// URLSession delegate、ping callback、超时与任务取消可能从不同线程同时到达；
/// 使用锁保证 continuation 至多恢复一次，同时支持“先回调、后 await”的握手时序。
final class WebSocketAsyncGate<Value: Sendable>: @unchecked Sendable {
    private typealias Outcome = Result<Value, any Error>

    private let lock = NSLock()
    private var outcome: Outcome?
    private var continuation: CheckedContinuation<Value, any Error>?
    private var hasWaiter = false

    @discardableResult
    func resolve(_ outcome: Result<Value, any Error>) -> Bool {
        let resolution: (didResolve: Bool, continuation: CheckedContinuation<Value, any Error>?) = lock.withLock {
            guard self.outcome == nil else { return (false, nil) }
            self.outcome = outcome
            let continuation = self.continuation
            self.continuation = nil
            return (true, continuation)
        }

        guard resolution.didResolve else { return false }
        if let continuation = resolution.continuation {
            continuation.resume(with: outcome)
        }
        return true
    }

    func wait() async throws -> Value {
        try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                let immediateOutcome: Outcome? = lock.withLock {
                    guard !hasWaiter else {
                        return .failure(WebSocketTransportError.duplicateWaiter)
                    }
                    hasWaiter = true
                    if let outcome {
                        return outcome
                    }
                    self.continuation = continuation
                    return nil
                }

                if let immediateOutcome {
                    continuation.resume(with: immediateOutcome)
                }
            }
        } onCancel: {
            self.resolve(.failure(CancellationError()))
        }
    }
}

/// 将 URLSession delegate 事件桥接到单次握手等待和 actor 状态机。
///
/// delegate 可能先后收到 `didClose` 与 `didCompleteWithError`，terminationHandler
/// 必须只通知一次；actor 侧还会再次校验 generation，形成两层防线。
final class WebSocketSessionDelegate: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
    typealias TerminationHandler = @Sendable (UInt64, any Error) -> Void

    let generation: UInt64
    let handshakeGate: WebSocketAsyncGate<Void>

    private let lock = NSLock()
    private let terminationHandler: TerminationHandler
    private var didNotifyTermination = false

    init(
        generation: UInt64,
        handshakeGate: WebSocketAsyncGate<Void>,
        terminationHandler: @escaping TerminationHandler
    ) {
        self.generation = generation
        self.handshakeGate = handshakeGate
        self.terminationHandler = terminationHandler
    }

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didOpenWithProtocol protocol: String?
    ) {
        handshakeGate.resolve(.success(()))
    }

    func urlSession(
        _ session: URLSession,
        webSocketTask: URLSessionWebSocketTask,
        didCloseWith closeCode: URLSessionWebSocketTask.CloseCode,
        reason: Data?
    ) {
        let reasonText = reason.flatMap { String(data: $0, encoding: .utf8) }
        let error = WebSocketTransportError.closed(
            code: closeCode.rawValue,
            reason: reasonText
        )
        handshakeGate.resolve(.failure(error))
        notifyTerminationOnce(error)
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didCompleteWithError error: (any Error)?
    ) {
        guard let webSocketTask = task as? URLSessionWebSocketTask else { return }

        // 部分系统/代理链只回调 didComplete，没有先回调 didClose。此时仍从 task 提取
        // closeCode/closeReason，避免上层最终只看到笼统的 transportCompleted。
        let terminalError: any Error
        if webSocketTask.closeCode != .invalid {
            terminalError = WebSocketTransportError.closed(
                code: webSocketTask.closeCode.rawValue,
                reason: webSocketTask.closeReason.flatMap { String(data: $0, encoding: .utf8) }
            )
        } else {
            terminalError = error ?? WebSocketTransportError.transportCompleted
        }
        handshakeGate.resolve(.failure(terminalError))
        notifyTerminationOnce(terminalError)
    }

    private func notifyTerminationOnce(_ error: any Error) {
        let shouldNotify = lock.withLock {
            guard !didNotifyTermination else { return false }
            didNotifyTermination = true
            return true
        }
        guard shouldNotify else { return }
        terminationHandler(generation, error)
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

/// 最近一次远端 WebSocket 关闭信息。
///
/// `reason` 保留给业务层判断服务端下线、鉴权过期等语义，但不会写入网络层日志。
/// `code == 1000` 表示 RFC 6455 正常关闭，其余代码由调用方按协议决定是否重试或提示用户。
public struct WebSocketCloseInfo: Sendable, Equatable {
    public let code: Int
    public let reason: String?

    public init(code: Int, reason: String?) {
        self.code = code
        self.reason = reason
    }

    public var isNormalClosure: Bool {
        code == URLSessionWebSocketTask.CloseCode.normalClosure.rawValue
    }
}

/// WebSocket 客户端
///
/// 基于 `URLSessionWebSocketTask`，支持自动重连和 AsyncSequence 消息流。
/// 每个 `messageStream()` 调用者拥有独立、有限容量的缓冲区；慢消费者不会无限占用内存。
///
/// ```swift
/// let ws = WebSocketClient(url: URL(string: "wss://live.example.com/room/123")!)
/// let stream = await ws.messageStream() // 在 connect 前注册，避免遗漏首包
/// try await ws.connect()                // URLSession delegate 确认握手后才返回
/// try await ws.send(.text("{\"action\":\"join\"}"))
///
/// for await message in stream {
///     // handle message
/// }
/// ```
public final actor WebSocketClient {

    private struct Subscriber {
        let continuation: AsyncStream<WebSocketMessage>.Continuation
        var droppedMessageCount = 0
    }

    private let url: URL
    private let headers: [String: String]
    private let autoReconnect: Bool
    private let maxReconnectAttempts: Int
    private let reconnectBaseDelay: TimeInterval
    private let handshakeTimeout: TimeInterval
    private let pingTimeout: TimeInterval
    private let messageBufferLimit: Int
    private let sessionPool: SessionPool

    private var task: URLSessionWebSocketTask?
    private var session: URLSession?
    private var sessionDelegate: WebSocketSessionDelegate?
    private var connectionGeneration: UInt64 = 0
    private var activeGeneration: UInt64?
    private var reconnectCount = 0
    private var intentionalDisconnect = false
    private var receiveLoopTask: Task<Void, Never>?
    private var connectionAttempt: (generation: UInt64, task: Task<Void, any Error>)?
    private var reconnectTask: Task<Void, Never>?
    private var reconnectScheduleGeneration: UInt64 = 0

    /// 消息广播：每个监听者拥有独立的 bounded buffer。
    private var subscribers: [UUID: Subscriber] = [:]

    private(set) public var state: WebSocketState = .disconnected(reason: nil)
    /// 最近一次由远端发送的结构化 close 信息；重连不会立即清空，便于上层读取失败原因。
    private(set) public var lastRemoteClose: WebSocketCloseInfo?
    private var lastRemoteCloseGeneration: UInt64 = 0

    public init(
        url: URL,
        headers: [String: String] = [:],
        autoReconnect: Bool = true,
        maxReconnectAttempts: Int = 5,
        reconnectBaseDelay: TimeInterval = 1.0,
        sessionPool: SessionPool = .shared,
        handshakeTimeout: TimeInterval = 15.0,
        pingTimeout: TimeInterval = 10.0,
        messageBufferLimit: Int = 256
    ) {
        self.url = url
        self.headers = headers
        self.autoReconnect = autoReconnect
        self.maxReconnectAttempts = max(0, maxReconnectAttempts)
        self.reconnectBaseDelay = max(0, reconnectBaseDelay)
        self.sessionPool = sessionPool
        self.handshakeTimeout = max(0.1, handshakeTimeout)
        self.pingTimeout = max(0.1, pingTimeout)
        self.messageBufferLimit = max(1, messageBufferLimit)
    }

    // MARK: - Connect / Disconnect

    /// 建立连接；只有 URLSession delegate 收到握手成功回调后才返回。
    ///
    /// 并发调用会共享同一个连接尝试，避免重复创建 transport。手动调用会取消尚未开始的
    /// 自动重连倒计时并重置重连预算，但不会打断已在进行的同代握手。
    public func connect() async throws {
        intentionalDisconnect = false
        cancelScheduledReconnect(reason: "manual connect")

        if state == .connected, task != nil {
            return
        }
        if let connectionAttempt {
            try await connectionAttempt.task.value
            return
        }

        reconnectCount = 0
        try await runConnectionAttempt()
    }

    /// 断开连接并终止现有消息流。
    ///
    /// generation 先递增再取消 transport，保证晚到的 delegate/receive/ping 回调都只能被忽略。
    public func disconnect(reason: String? = nil) {
        intentionalDisconnect = true
        connectionGeneration &+= 1
        cancelScheduledReconnect(reason: "intentional disconnect")

        connectionAttempt?.task.cancel()
        connectionAttempt = nil
        closeActiveTransport(
            reason: reason ?? "intentional",
            closeCode: .normalClosure
        )
        finishAllStreams()
    }

    // MARK: - Send

    /// 发送消息。发送失败会使当前 generation 进入断线/重连路径。
    public func send(_ message: WebSocketMessage) async throws {
        guard let task, let generation = activeGeneration, state == .connected else {
            throw notConnectedError()
        }

        do {
            switch message {
            case .text(let string):
                try await task.send(.string(string))
            case .data(let data):
                try await task.send(.data(data))
            }

            guard activeGeneration == generation, self.task === task, state == .connected else {
                throw CancellationError()
            }
        } catch is CancellationError {
            // 调用方取消一次 send 不代表 transport 已断开；只把取消传回上层。
            throw CancellationError()
        } catch {
            handleTransportFailure(
                generation: generation,
                error: error,
                source: "send"
            )
            throw error
        }
    }

    // MARK: - Message Stream

    /// 创建消息接收 AsyncStream（可多次调用，每个调用者获得独立 stream）。
    ///
    /// 使用 `.bufferingNewest(messageBufferLimit)`：消费者卡顿时保留最新消息并丢弃旧消息，
    /// 避免直播弹幕等高频流在后台或主线程阻塞时形成无界内存增长。
    public func messageStream() -> AsyncStream<WebSocketMessage> {
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(
            of: WebSocketMessage.self,
            bufferingPolicy: .bufferingNewest(messageBufferLimit)
        )
        subscribers[id] = Subscriber(continuation: continuation)
        continuation.onTermination = { [weak self] _ in
            Task { [weak self] in
                await self?.removeSubscriber(id: id)
            }
        }
        return stream
    }

    private func removeSubscriber(id: UUID) {
        subscribers.removeValue(forKey: id)
    }

    /// 收到首条业务数据后才把连接视为稳定，防止“握手成功后立刻断开”反复绕过重连上限。
    private func resetReconnectCount(generation: UInt64) {
        guard activeGeneration == generation, reconnectCount != 0 else { return }
        logger.info(
            "WebSocket stable after reconnect; reset retry budget: generation=\(generation), previous=\(self.reconnectCount)"
        )
        reconnectCount = 0
    }

    // MARK: - Ping

    /// 发送 ping，并使用独立超时防止 URLSession callback 永久不返回。
    ///
    /// 超时意味着当前 transport 已无法被可靠判活，会立即进入统一断线/重连路径。
    public func ping() async throws {
        guard let task, let generation = activeGeneration, state == .connected else {
            logger.warning(
                "WebSocket ping rejected: state=not-connected, host=\(self.url.host ?? "unknown")"
            )
            throw notConnectedError()
        }

        let gate = WebSocketAsyncGate<Void>()
        let timeout = pingTimeout
        let timeoutTask = Task {
            do {
                try await Task.sleep(for: .seconds(timeout))
            } catch {
                return
            }
            gate.resolve(.failure(WebSocketTransportError.pingTimedOut(seconds: timeout)))
        }
        defer { timeoutTask.cancel() }

        task.sendPing { error in
            if let error {
                gate.resolve(.failure(error))
            } else {
                gate.resolve(.success(()))
            }
        }

        do {
            try await gate.wait()
            guard activeGeneration == generation, self.task === task, state == .connected else {
                throw CancellationError()
            }
        } catch is CancellationError {
            // 页面/心跳任务被取消时不应误触发自动重连；真正的断开由 disconnect 负责。
            throw CancellationError()
        } catch {
            logger.error(
                "WebSocket ping failed: generation=\(generation), host=\(self.url.host ?? "unknown"), category=\(Self.diagnosticCategory(for: error))"
            )
            handleTransportFailure(
                generation: generation,
                error: error,
                source: "ping"
            )
            throw error
        }
    }

    // MARK: - Connection Attempt

    private func runConnectionAttempt() async throws {
        if let connectionAttempt {
            try await connectionAttempt.task.value
            return
        }

        connectionGeneration &+= 1
        let generation = connectionGeneration
        let attempt = Task { [weak self] in
            guard let self else { throw CancellationError() }
            try await self.establishConnection(generation: generation)
        }
        connectionAttempt = (generation, attempt)

        do {
            try await attempt.value
            clearConnectionAttempt(generation: generation)
        } catch {
            clearConnectionAttempt(generation: generation)
            throw error
        }
    }

    private func clearConnectionAttempt(generation: UInt64) {
        guard connectionAttempt?.generation == generation else { return }
        connectionAttempt = nil
    }

    private func establishConnection(generation: UInt64) async throws {
        try Task.checkCancellation()
        guard generation == connectionGeneration, !intentionalDisconnect else {
            throw CancellationError()
        }

        var request = URLRequest(url: url)
        for (key, value) in headers {
            request.setValue(value, forHTTPHeaderField: key)
        }

        let handshakeGate = WebSocketAsyncGate<Void>()
        let delegate = WebSocketSessionDelegate(
            generation: generation,
            handshakeGate: handshakeGate
        ) { [weak self] generation, error in
            Task { [weak self] in
                await self?.transportDidTerminate(
                    generation: generation,
                    error: error
                )
            }
        }
        let session = sessionPool.makeDelegateSession(
            config: .websocket,
            delegate: delegate
        )
        let webSocketTask = session.webSocketTask(with: request)

        // 字段在 resume 前一次性登记，delegate 即使同步/极快回调也能找到正确 generation。
        activeGeneration = generation
        sessionDelegate = delegate
        self.session = session
        task = webSocketTask
        state = .connecting

        webSocketTask.resume()
        logger.info(
            "WebSocket handshake started: generation=\(generation), host=\(self.url.host ?? "unknown")"
        )

        let timeout = handshakeTimeout
        let timeoutTask = Task {
            do {
                try await Task.sleep(for: .seconds(timeout))
            } catch {
                return
            }
            handshakeGate.resolve(
                .failure(WebSocketTransportError.handshakeTimedOut(seconds: timeout))
            )
        }
        defer { timeoutTask.cancel() }

        do {
            try await handshakeGate.wait()
            try Task.checkCancellation()
        } catch is CancellationError {
            handleTransportFailure(
                generation: generation,
                error: CancellationError(),
                source: "cancelled handshake"
            )
            throw CancellationError()
        } catch {
            handleTransportFailure(
                generation: generation,
                error: error,
                source: "handshake"
            )
            throw NetworkError.transport(
                underlying: error,
                requestID: UUID().uuidString
            )
        }

        guard activeGeneration == generation,
              connectionGeneration == generation,
              self.task === webSocketTask,
              !intentionalDisconnect else {
            let error = CancellationError()
            handleTransportFailure(
                generation: generation,
                error: error,
                source: "stale handshake completion"
            )
            throw error
        }

        state = .connected
        logger.info(
            "WebSocket handshake completed: generation=\(generation), host=\(self.url.host ?? "unknown")"
        )
        startReceiveLoop(task: webSocketTask, generation: generation)
    }

    // MARK: - Receive / Broadcast

    private func startReceiveLoop(task: URLSessionWebSocketTask, generation: UInt64) {
        receiveLoopTask?.cancel()
        receiveLoopTask = Task { [weak self] in
            var didMarkStable = false
            while !Task.isCancelled {
                do {
                    let message = try await task.receive()
                    guard !Task.isCancelled, let self else { break }

                    let shouldContinue = await self.isActiveTransport(
                        generation: generation,
                        task: task
                    )
                    guard shouldContinue else { break }

                    if !didMarkStable {
                        didMarkStable = true
                        await self.resetReconnectCount(generation: generation)
                    }

                    switch message {
                    case .string(let text):
                        await self.broadcast(.text(text), generation: generation)
                    case .data(let data):
                        await self.broadcast(.data(data), generation: generation)
                    @unknown default:
                        logger.warning(
                            "WebSocket ignored unknown message: generation=\(generation)"
                        )
                    }
                } catch {
                    guard !Task.isCancelled, let self else { break }
                    await self.handleTransportFailure(
                        generation: generation,
                        error: error,
                        source: "receive"
                    )
                    break
                }
            }
        }
    }

    private func isActiveTransport(
        generation: UInt64,
        task expectedTask: URLSessionWebSocketTask
    ) -> Bool {
        activeGeneration == generation && task === expectedTask && state == .connected
    }

    private func broadcast(_ message: WebSocketMessage, generation: UInt64) {
        guard activeGeneration == generation, state == .connected else {
            logger.debug(
                "WebSocket dropped stale transport message: generation=\(generation), active=\(self.activeGeneration ?? 0)"
            )
            return
        }

        // 使用 key 快照，避免 yield 触发 termination 后在遍历期间修改字典。
        for id in Array(subscribers.keys) {
            guard var subscriber = subscribers[id] else { continue }
            switch subscriber.continuation.yield(message) {
            case .enqueued:
                break
            case .dropped:
                subscriber.droppedMessageCount += 1
                subscribers[id] = subscriber
                // 首次和 2 的幂次打印，既能定位慢消费者又不会形成日志风暴。
                let count = subscriber.droppedMessageCount
                if count == 1 || (count & (count - 1)) == 0 {
                    logger.warning(
                        "WebSocket subscriber buffer overflow: subscriber=\(id.uuidString), dropped=\(count), limit=\(self.messageBufferLimit)"
                    )
                }
            case .terminated:
                subscribers.removeValue(forKey: id)
            @unknown default:
                logger.warning(
                    "WebSocket stream returned unknown yield result: subscriber=\(id.uuidString)"
                )
            }
        }
    }

    // MARK: - Failure / Reconnect

    private func transportDidTerminate(generation: UInt64, error: any Error) {
        handleTransportFailure(
            generation: generation,
            error: error,
            source: "URLSession delegate"
        )
    }

    private func handleTransportFailure(
        generation: UInt64,
        error: any Error,
        source: String
    ) {
        // receive / delegate 回调存在竞争：通用 receive 错误可能先关闭 transport，随后才收到
        // 带 code/reason 的 didClose。即使后者已变成 stale，也应补录同代结构化关闭信息；
        // generation 检查同时防止更旧的迟到回调覆盖新连接的关闭原因。
        if let closeInfo = (error as? WebSocketTransportError)?.closeInfo,
           generation >= lastRemoteCloseGeneration {
            lastRemoteClose = closeInfo
            lastRemoteCloseGeneration = generation
        }

        guard activeGeneration == generation else {
            logger.debug(
                "WebSocket ignored stale failure: source=\(source), generation=\(generation), active=\(self.activeGeneration ?? 0), category=\(Self.diagnosticCategory(for: error))"
            )
            return
        }

        let reason = error.localizedDescription
        logger.error(
            "WebSocket transport failed: source=\(source), generation=\(generation), category=\(Self.diagnosticCategory(for: error))"
        )
        // 失败回调可能早于 runConnectionAttempt 的 await/catch 返回（尤其重连延迟为 0 时）。
        // 先释放同代 attempt，确保马上触发的重连不会再次等待已经失败的旧 Task。
        if connectionAttempt?.generation == generation {
            connectionAttempt = nil
        }
        closeActiveTransport(reason: reason, closeCode: .goingAway)

        guard !intentionalDisconnect,
              autoReconnect,
              reconnectCount < maxReconnectAttempts else {
            logger.info(
                "WebSocket reconnect stopped: intentional=\(self.intentionalDisconnect), enabled=\(self.autoReconnect), count=\(self.reconnectCount), max=\(self.maxReconnectAttempts)"
            )
            finishAllStreams()
            return
        }

        scheduleReconnect()
    }

    private func scheduleReconnect() {
        guard reconnectTask == nil else {
            logger.debug(
                "WebSocket reconnect already scheduled; duplicate ignored"
            )
            return
        }

        reconnectCount += 1
        let attemptNumber = reconnectCount
        let delay = reconnectBaseDelay * pow(2, Double(attemptNumber - 1))
        reconnectScheduleGeneration &+= 1
        let scheduleGeneration = reconnectScheduleGeneration

        logger.info(
            "WebSocket reconnect scheduled: attempt=\(attemptNumber)/\(self.maxReconnectAttempts), delay=\(String(format: "%.1f", delay))s, schedule=\(scheduleGeneration)"
        )

        reconnectTask = Task { [weak self] in
            do {
                try await Task.sleep(for: .seconds(delay))
            } catch {
                return
            }
            guard let self else { return }
            await self.performScheduledReconnect(
                scheduleGeneration: scheduleGeneration,
                attemptNumber: attemptNumber
            )
        }
    }

    private func performScheduledReconnect(
        scheduleGeneration: UInt64,
        attemptNumber: Int
    ) async {
        let isCurrentSchedule = reconnectScheduleGeneration == scheduleGeneration
            && reconnectTask != nil
        guard isCurrentSchedule,
              !intentionalDisconnect,
              state != .connected else {
            if isCurrentSchedule {
                reconnectTask = nil
            }
            logger.debug(
                "WebSocket skipped stale reconnect: schedule=\(scheduleGeneration), current=\(self.reconnectScheduleGeneration), intentional=\(self.intentionalDisconnect)"
            )
            return
        }

        // 先清空当前计划；若本次握手失败，failure path 才能登记下一次且不会被误判为重复。
        reconnectTask = nil
        do {
            try await runConnectionAttempt()
        } catch is CancellationError {
            logger.info(
                "WebSocket reconnect cancelled: attempt=\(attemptNumber), schedule=\(scheduleGeneration)"
            )
        } catch {
            // establishConnection 已负责清理和安排下一次重连，此处仅补足诊断。
            logger.error(
                "WebSocket reconnect attempt failed: attempt=\(attemptNumber), schedule=\(scheduleGeneration), category=\(Self.diagnosticCategory(for: error))"
            )
        }
    }

    private func cancelScheduledReconnect(reason: String) {
        reconnectScheduleGeneration &+= 1
        guard let reconnectTask else { return }
        reconnectTask.cancel()
        self.reconnectTask = nil
        logger.info(
            "WebSocket reconnect cancelled: reason=\(reason), schedule=\(self.reconnectScheduleGeneration)"
        )
    }

    // MARK: - Cleanup

    private func closeActiveTransport(
        reason: String,
        closeCode: URLSessionWebSocketTask.CloseCode
    ) {
        let oldTask = task
        let oldSession = session
        let oldGeneration = activeGeneration

        activeGeneration = nil
        task = nil
        session = nil
        sessionDelegate = nil
        receiveLoopTask?.cancel()
        receiveLoopTask = nil
        state = .disconnected(reason: reason)

        // Task.cancel + session invalidation 能解除挂起中的 receive，不需要等待接收循环退出。
        oldTask?.cancel(with: closeCode, reason: reason.data(using: .utf8))
        oldSession?.invalidateAndCancel()
        logger.info(
            "WebSocket transport closed: generation=\(oldGeneration ?? 0)"
        )
    }

    private func finishAllStreams() {
        guard !subscribers.isEmpty else { return }
        let subscriberCount = subscribers.count
        for subscriber in subscribers.values {
            subscriber.continuation.finish()
        }
        subscribers.removeAll()
        logger.info(
            "WebSocket streams finished: subscribers=\(subscriberCount)"
        )
    }

    /// 日志只保留稳定类别和标准错误码，不记录 URL path、close reason 或系统错误正文。
    private static func diagnosticCategory(for error: any Error) -> String {
        if error is CancellationError {
            return "cancelled"
        }
        if let transportError = error as? WebSocketTransportError {
            switch transportError {
            case .handshakeTimedOut:
                return "handshake-timeout"
            case .pingTimedOut:
                return "ping-timeout"
            case .closed(let code, _):
                return "remote-close-\(code)"
            case .transportCompleted:
                return "transport-completed"
            case .duplicateWaiter:
                return "duplicate-waiter"
            }
        }
        if let urlError = error as? URLError {
            return "url-error-\(urlError.code.rawValue)"
        }
        return "transport"
    }

    private func notConnectedError() -> NetworkError {
        NetworkError.transport(
            underlying: URLError(.notConnectedToInternet),
            requestID: UUID().uuidString
        )
    }

    deinit {
        connectionAttempt?.task.cancel()
        reconnectTask?.cancel()
        receiveLoopTask?.cancel()
        task?.cancel(with: .goingAway, reason: nil)
        session?.invalidateAndCancel()
    }
}
