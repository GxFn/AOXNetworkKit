import CryptoKit
import Foundation
import Network
@testable import AOXNetworkKit

/// 仅供测试使用的 loopback RFC 6455 服务。
///
/// 夹具直接监听 `127.0.0.1`，不会访问外网；所有可观察时序都通过事件流和 deadline
/// 推进，不依赖固定 sleep 猜测 URLSession 的回调顺序。
actor LocalRFC6455Server {
    enum HandshakePolicy: Sendable {
        case accept
        case rejectForbidden
        case rejectTLSClientHello
    }

    enum Event: Sendable, Equatable {
        case handshakeAccepted(connectionID: Int, path: String)
        case handshakeRejected(connectionID: Int, statusCode: Int)
        case tlsClientHelloRejected(connectionID: Int, hasHandshakeRecord: Bool)
        case textReceived(connectionID: Int, text: String)
        case binaryReceived(connectionID: Int, data: Data)
        case closeReceived(connectionID: Int, code: Int, reason: String?)
        case connectionEnded(connectionID: Int)
    }

    private enum ConnectionPhase: Sendable {
        case readingHTTP
        case sendingUpgrade(path: String)
        case open
        case rejecting
    }

    private struct ConnectionRecord: Sendable {
        let connection: NWConnection
        var phase: ConnectionPhase = .readingHTTP
        var receiveBuffer = Data()
    }

    private struct DecodedFrame: Sendable {
        let opcode: UInt8
        let payload: Data
    }

    private enum ServerError: Error, Sendable, Equatable {
        case listenerReadyTimedOut
        case listenerCancelledBeforeReady
        case listenerHasNoPort
        case connectionNotOpen(Int)
        case sendTimedOut(Int)
        case malformedHandshake
        case oversizedHandshake
        case unmaskedClientFrame
        case fragmentedClientFrame
        case oversizedFrame
        case malformedFrame
    }

    private static let websocketGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
    private static let headerTerminator = Data("\r\n\r\n".utf8)
    private static let maximumHandshakeBytes = 16 * 1024
    private static let maximumFrameBytes = 1024 * 1024

    private let policy: HandshakePolicy
    private let listener: NWListener
    private let queue = DispatchQueue(label: "com.aoxnetworkkit.tests.rfc6455-server")
    private let readyGate = WebSocketAsyncGate<UInt16>()
    private let eventContinuation: AsyncStream<Event>.Continuation
    private let eventStream: AsyncStream<Event>

    private var nextConnectionID = 0
    private var connections: [Int: ConnectionRecord] = [:]
    private(set) var acceptedHandshakeCount = 0
    private var didStart = false
    private var didStop = false

    init(policy: HandshakePolicy = .accept) throws {
        self.policy = policy

        let parameters = NWParameters.tcp
        parameters.allowLocalEndpointReuse = true
        parameters.requiredLocalEndpoint = .hostPort(
            host: NWEndpoint.Host("127.0.0.1"),
            port: .any
        )
        listener = try NWListener(using: parameters)

        let pair = AsyncStream.makeStream(
            of: Event.self,
            bufferingPolicy: .unbounded
        )
        eventStream = pair.stream
        eventContinuation = pair.continuation
    }

    func events() -> AsyncStream<Event> {
        eventStream
    }

    /// 启动监听并在明确 deadline 内等待系统分配端口。
    func start(timeout: Duration = .seconds(3)) async throws -> UInt16 {
        guard !didStart else {
            return try await readyGate.wait()
        }
        didStart = true

        listener.stateUpdateHandler = { [weak self] state in
            Task { [weak self] in
                await self?.handleListenerState(state)
            }
        }
        listener.newConnectionHandler = { [weak self] connection in
            Task { [weak self] in
                await self?.accept(connection)
            }
        }
        listener.start(queue: queue)

        let deadlineTask = Task { [readyGate] in
            do {
                try await Task.sleep(for: timeout)
            } catch {
                return
            }
            readyGate.resolve(.failure(ServerError.listenerReadyTimedOut))
        }
        defer { deadlineTask.cancel() }

        return try await readyGate.wait()
    }

    func stop() {
        guard !didStop else { return }
        didStop = true

        listener.cancel()
        readyGate.resolve(.failure(ServerError.listenerCancelledBeforeReady))
        for record in connections.values {
            record.connection.cancel()
        }
        connections.removeAll()
        eventContinuation.finish()
    }

    func sendText(_ text: String, to connectionID: Int) async throws {
        try await sendFrame(
            opcode: 0x1,
            payload: Data(text.utf8),
            to: connectionID
        )
    }

    func sendBinary(_ data: Data, to connectionID: Int) async throws {
        try await sendFrame(opcode: 0x2, payload: data, to: connectionID)
    }

    func sendClose(code: Int, reason: String?, to connectionID: Int) async throws {
        var payload = Data()
        payload.append(UInt8((code >> 8) & 0xff))
        payload.append(UInt8(code & 0xff))
        if let reason {
            payload.append(Data(reason.utf8))
        }
        guard payload.count <= 125 else {
            throw ServerError.oversizedFrame
        }
        try await sendFrame(opcode: 0x8, payload: payload, to: connectionID)
    }

    // MARK: - Listener

    private func handleListenerState(_ state: NWListener.State) {
        switch state {
        case .ready:
            guard let port = listener.port else {
                readyGate.resolve(.failure(ServerError.listenerHasNoPort))
                return
            }
            readyGate.resolve(.success(port.rawValue))

        case .failed(let error):
            readyGate.resolve(.failure(error))

        case .cancelled:
            readyGate.resolve(.failure(ServerError.listenerCancelledBeforeReady))

        case .setup, .waiting:
            break

        @unknown default:
            break
        }
    }

    private func accept(_ connection: NWConnection) {
        nextConnectionID += 1
        let connectionID = nextConnectionID
        connections[connectionID] = ConnectionRecord(connection: connection)

        connection.stateUpdateHandler = { [weak self] state in
            switch state {
            case .failed, .cancelled:
                Task { [weak self] in
                    await self?.finishConnection(connectionID)
                }
            default:
                break
            }
        }
        connection.start(queue: queue)
        receiveMore(connectionID)
    }

    private func finishConnection(_ connectionID: Int) {
        guard connections.removeValue(forKey: connectionID) != nil else { return }
        eventContinuation.yield(.connectionEnded(connectionID: connectionID))
    }

    // MARK: - Receive

    private func receiveMore(_ connectionID: Int) {
        guard let record = connections[connectionID], !didStop else { return }
        record.connection.receive(
            minimumIncompleteLength: 1,
            maximumLength: 64 * 1024
        ) { [weak self] data, _, isComplete, error in
            Task { [weak self] in
                await self?.didReceive(
                    data,
                    isComplete: isComplete,
                    error: error,
                    connectionID: connectionID
                )
            }
        }
    }

    private func didReceive(
        _ data: Data?,
        isComplete: Bool,
        error: NWError?,
        connectionID: Int
    ) {
        guard var record = connections[connectionID] else { return }

        if policy == .rejectTLSClientHello,
           case .readingHTTP = record.phase,
           let data,
           let firstByte = data.first {
            // TLS Handshake record 的 content type 是 0x16。
            // 只记录布尔判断，不保存或输出 ClientHello。
            eventContinuation.yield(
                .tlsClientHelloRejected(
                    connectionID: connectionID,
                    hasHandshakeRecord: firstByte == 0x16
                )
            )
            record.connection.cancel()
            connections.removeValue(forKey: connectionID)
            return
        }

        if let data, !data.isEmpty {
            record.receiveBuffer.append(data)
            connections[connectionID] = record
        }

        if error != nil || isComplete {
            record.connection.cancel()
            finishConnection(connectionID)
            return
        }

        switch record.phase {
        case .readingHTTP:
            processHandshake(connectionID)

        case .open:
            processFrames(connectionID)
            receiveMore(connectionID)

        case .sendingUpgrade, .rejecting:
            // 握手响应 completion 会继续推进读取，避免同一连接并发登记两个 receive。
            break
        }
    }

    // MARK: - Handshake

    private func processHandshake(_ connectionID: Int) {
        guard var record = connections[connectionID] else { return }
        guard let delimiterRange = record.receiveBuffer.range(of: Self.headerTerminator) else {
            if record.receiveBuffer.count > Self.maximumHandshakeBytes {
                rejectMalformedHandshake(connectionID, record: record)
            } else {
                receiveMore(connectionID)
            }
            return
        }

        let headerData = record.receiveBuffer[..<delimiterRange.lowerBound]
        let remainder = record.receiveBuffer[delimiterRange.upperBound...]
        record.receiveBuffer = Data(remainder)

        guard let headerText = String(data: headerData, encoding: .utf8),
              let request = Self.parseHandshake(headerText) else {
            rejectMalformedHandshake(connectionID, record: record)
            return
        }

        switch policy {
        case .rejectForbidden:
            record.phase = .rejecting
            connections[connectionID] = record
            sendHTTPResponse(
                "HTTP/1.1 403 Forbidden\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
                connectionID: connectionID,
                statusCode: 403
            )

        case .accept:
            let acceptValue = Self.websocketAcceptValue(for: request.key)
            record.phase = .sendingUpgrade(path: request.path)
            connections[connectionID] = record
            let response = """
            HTTP/1.1 101 Switching Protocols\r
            Upgrade: websocket\r
            Connection: Upgrade\r
            Sec-WebSocket-Accept: \(acceptValue)\r
            \r

            """
            sendUpgradeResponse(
                Data(response.utf8),
                connectionID: connectionID,
                path: request.path
            )

        case .rejectTLSClientHello:
            // ClientHello 在 HTTP 解析前已被拒绝；若走到这里，说明客户端未发送 TLS。
            record.connection.cancel()
            finishConnection(connectionID)
        }
    }

    private func rejectMalformedHandshake(
        _ connectionID: Int,
        record: ConnectionRecord
    ) {
        var record = record
        record.phase = .rejecting
        connections[connectionID] = record
        sendHTTPResponse(
            "HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            connectionID: connectionID,
            statusCode: 400
        )
    }

    private func sendHTTPResponse(
        _ response: String,
        connectionID: Int,
        statusCode: Int
    ) {
        guard let connection = connections[connectionID]?.connection else { return }
        connection.send(
            content: Data(response.utf8),
            completion: .contentProcessed { [weak self] _ in
                Task { [weak self] in
                    await self?.completeHTTPRejection(
                        connectionID: connectionID,
                        statusCode: statusCode
                    )
                }
            }
        )
    }

    private func completeHTTPRejection(connectionID: Int, statusCode: Int) {
        guard let record = connections.removeValue(forKey: connectionID) else { return }
        eventContinuation.yield(
            .handshakeRejected(connectionID: connectionID, statusCode: statusCode)
        )
        record.connection.cancel()
    }

    private func sendUpgradeResponse(
        _ response: Data,
        connectionID: Int,
        path: String
    ) {
        guard let connection = connections[connectionID]?.connection else { return }
        connection.send(
            content: response,
            completion: .contentProcessed { [weak self] error in
                Task { [weak self] in
                    await self?.completeUpgrade(
                        connectionID: connectionID,
                        path: path,
                        error: error
                    )
                }
            }
        )
    }

    private func completeUpgrade(
        connectionID: Int,
        path: String,
        error: NWError?
    ) {
        guard var record = connections[connectionID] else { return }
        guard error == nil else {
            record.connection.cancel()
            finishConnection(connectionID)
            return
        }

        record.phase = .open
        connections[connectionID] = record
        acceptedHandshakeCount += 1
        eventContinuation.yield(
            .handshakeAccepted(connectionID: connectionID, path: path)
        )
        processFrames(connectionID)
        receiveMore(connectionID)
    }

    private static func parseHandshake(_ text: String) -> (path: String, key: String)? {
        let lines = text.components(separatedBy: "\r\n")
        guard let requestLine = lines.first else { return nil }
        let requestParts = requestLine.split(separator: " ")
        guard requestParts.count == 3,
              requestParts[0] == "GET",
              requestParts[2] == "HTTP/1.1" else {
            return nil
        }

        var headers: [String: String] = [:]
        for line in lines.dropFirst() {
            guard let separator = line.firstIndex(of: ":") else { continue }
            let name = line[..<separator]
                .trimmingCharacters(in: .whitespacesAndNewlines)
                .lowercased()
            let value = line[line.index(after: separator)...]
                .trimmingCharacters(in: .whitespacesAndNewlines)
            headers[name] = value
        }

        guard headers["upgrade"]?.lowercased() == "websocket",
              headers["connection"]?.lowercased().contains("upgrade") == true,
              headers["sec-websocket-version"] == "13",
              let key = headers["sec-websocket-key"],
              !key.isEmpty else {
            return nil
        }
        return (String(requestParts[1]), key)
    }

    private static func websocketAcceptValue(for key: String) -> String {
        let digest = Insecure.SHA1.hash(data: Data((key + websocketGUID).utf8))
        return Data(digest).base64EncodedString()
    }

    // MARK: - Frames

    private func sendFrame(
        opcode: UInt8,
        payload: Data,
        to connectionID: Int,
        timeout: Duration = .seconds(3)
    ) async throws {
        guard let record = connections[connectionID], case .open = record.phase else {
            throw ServerError.connectionNotOpen(connectionID)
        }

        let gate = WebSocketAsyncGate<Void>()
        record.connection.send(
            content: Self.encodeServerFrame(opcode: opcode, payload: payload),
            completion: .contentProcessed { error in
                if let error {
                    gate.resolve(.failure(error))
                } else {
                    gate.resolve(.success(()))
                }
            }
        )

        let deadlineTask = Task {
            do {
                try await Task.sleep(for: timeout)
            } catch {
                return
            }
            gate.resolve(.failure(ServerError.sendTimedOut(connectionID)))
        }
        defer { deadlineTask.cancel() }
        try await gate.wait()
    }

    private func processFrames(_ connectionID: Int) {
        guard var record = connections[connectionID], case .open = record.phase else { return }

        do {
            while let frame = try Self.decodeClientFrame(from: &record.receiveBuffer) {
                switch frame.opcode {
                case 0x1:
                    guard let text = String(data: frame.payload, encoding: .utf8) else {
                        throw ServerError.malformedFrame
                    }
                    eventContinuation.yield(
                        .textReceived(connectionID: connectionID, text: text)
                    )

                case 0x2:
                    eventContinuation.yield(
                        .binaryReceived(connectionID: connectionID, data: frame.payload)
                    )

                case 0x8:
                    let close = try Self.decodeClosePayload(frame.payload)
                    eventContinuation.yield(
                        .closeReceived(
                            connectionID: connectionID,
                            code: close.code,
                            reason: close.reason
                        )
                    )

                case 0x9:
                    // RFC 6455 要求 pong 复用 ping payload；它是控制帧，不进入业务事件流。
                    record.connection.send(
                        content: Self.encodeServerFrame(opcode: 0xA, payload: frame.payload),
                        completion: .idempotent
                    )

                case 0xA:
                    break

                default:
                    throw ServerError.malformedFrame
                }
            }
            connections[connectionID] = record
        } catch {
            record.connection.cancel()
            connections.removeValue(forKey: connectionID)
            eventContinuation.yield(.connectionEnded(connectionID: connectionID))
        }
    }

    private static func encodeServerFrame(opcode: UInt8, payload: Data) -> Data {
        var frame = Data([0x80 | (opcode & 0x0f)])
        switch payload.count {
        case 0...125:
            frame.append(UInt8(payload.count))

        case 126...65_535:
            frame.append(126)
            frame.append(UInt8((payload.count >> 8) & 0xff))
            frame.append(UInt8(payload.count & 0xff))

        default:
            frame.append(127)
            let length = UInt64(payload.count)
            for shift in stride(from: 56, through: 0, by: -8) {
                frame.append(UInt8((length >> UInt64(shift)) & 0xff))
            }
        }
        frame.append(payload)
        return frame
    }

    private static func decodeClientFrame(from buffer: inout Data) throws -> DecodedFrame? {
        let bytes = [UInt8](buffer)
        guard bytes.count >= 2 else { return nil }

        let isFinal = (bytes[0] & 0x80) != 0
        guard isFinal else { throw ServerError.fragmentedClientFrame }
        let opcode = bytes[0] & 0x0f
        let isMasked = (bytes[1] & 0x80) != 0
        guard isMasked else { throw ServerError.unmaskedClientFrame }

        var cursor = 2
        var payloadLength = UInt64(bytes[1] & 0x7f)
        if payloadLength == 126 {
            guard bytes.count >= cursor + 2 else { return nil }
            payloadLength = (UInt64(bytes[cursor]) << 8) | UInt64(bytes[cursor + 1])
            cursor += 2
        } else if payloadLength == 127 {
            guard bytes.count >= cursor + 8 else { return nil }
            payloadLength = 0
            for index in cursor..<(cursor + 8) {
                payloadLength = (payloadLength << 8) | UInt64(bytes[index])
            }
            cursor += 8
        }

        guard payloadLength <= UInt64(maximumFrameBytes) else {
            throw ServerError.oversizedFrame
        }
        guard bytes.count >= cursor + 4 else { return nil }
        let mask = Array(bytes[cursor..<(cursor + 4)])
        cursor += 4

        let length = Int(payloadLength)
        guard bytes.count >= cursor + length else { return nil }
        var payloadBytes = Array(bytes[cursor..<(cursor + length)])
        for index in payloadBytes.indices {
            payloadBytes[index] ^= mask[index % 4]
        }

        buffer.removeFirst(cursor + length)
        return DecodedFrame(opcode: opcode, payload: Data(payloadBytes))
    }

    private static func decodeClosePayload(_ payload: Data) throws -> (code: Int, reason: String?) {
        guard !payload.isEmpty else { return (1005, nil) }
        guard payload.count >= 2 else { throw ServerError.malformedFrame }
        let bytes = [UInt8](payload)
        let code = (Int(bytes[0]) << 8) | Int(bytes[1])
        let reasonData = Data(bytes.dropFirst(2))
        let reason = reasonData.isEmpty ? nil : String(data: reasonData, encoding: .utf8)
        if !reasonData.isEmpty, reason == nil {
            throw ServerError.malformedFrame
        }
        return (code, reason)
    }
}
