// MARK: - Request Deduplicator

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "Dedup")

/// 对相同 key 的并发请求只执行一次 work，每个等待者独立处理取消。
/// 取消等待者不会取消共享 work；即使全部等待者退出，已开始的 work 仍完成并回收 key。
public final class RequestDeduplicator: Sendable {
    private final class Flight: Sendable {
        private typealias Outcome = Result<any Sendable, any Error>
        private typealias Waiter = CheckedContinuation<any Sendable, any Error>

        private struct State {
            var outcome: Outcome?
            var waiters: [UUID: Waiter] = [:]
        }

        private let state = OSAllocatedUnfairLock(initialState: State())

        func wait() async throws -> any Sendable {
            try Task.checkCancellation()
            let waiterID = UUID()
            return try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    let immediate: Outcome? = state.withLock { state in
                        // onCancel 可能先于 waiter 注册；注册时再次检查，避免遗失取消。
                        guard !Task.isCancelled else { return .failure(CancellationError()) }
                        if let outcome = state.outcome { return outcome }
                        state.waiters[waiterID] = continuation
                        return nil
                    }
                    if let immediate { continuation.resume(with: immediate) }
                }
            } onCancel: {
                self.cancelWaiter(waiterID)
            }
        }

        func finish(_ outcome: Result<any Sendable, any Error>) {
            let waiters: [Waiter] = state.withLock { state in
                guard state.outcome == nil else { return [] }
                state.outcome = outcome
                let waiters = Array(state.waiters.values)
                state.waiters.removeAll()
                return waiters
            }
            // 锁外恢复 continuation，完成与取消竞争时只会有一方取到对应 waiter。
            for waiter in waiters { waiter.resume(with: outcome) }
        }

        private func cancelWaiter(_ id: UUID) {
            let waiter = state.withLock { $0.waiters.removeValue(forKey: id) }
            guard let waiter else { return }
            waiter.resume(throwing: CancellationError())
            logger.debug("Dedup waiter cancelled; shared work continues")
        }
    }

    private let inFlight = OSAllocatedUnfairLock(initialState: [String: Flight]())

    public init() {}

    public func deduplicate<T: Sendable>(
        key: String,
        work: @Sendable @escaping () async throws -> T
    ) async throws -> T {
        try Task.checkCancellation()
        // leader 选择与发布在一个锁区间内，work 在锁外启动，不执行用户代码或 await。
        let (flight, isNew) = inFlight.withLock { flights -> (Flight, Bool) in
            if let existing = flights[key] { return (existing, false) }
            let flight = Flight()
            flights[key] = flight
            return (flight, true)
        }

        if isNew {
            // 共享任务独立于任一等待者；key 清理由真实 work 完成驱动，不能依赖可取消的 leader 等待者。
            Task {
                let outcome: Result<any Sendable, any Error>
                do {
                    outcome = .success(try await work())
                } catch {
                    outcome = .failure(error)
                }
                self.inFlight.withLock { flights in
                    if flights[key] === flight { flights.removeValue(forKey: key) }
                }
                flight.finish(outcome)
            }
        } else {
            // key 可能包含 query/签名，日志只记录共享路径，不输出请求身份正文。
            logger.debug("Dedup hit: sharing in-flight work")
        }

        let value = try await flight.wait()
        guard let typed = value as? T else {
            throw NetworkError.decoding(
                underlying: DeduplicatorTypeMismatch(expected: "\(T.self)", actual: "\(type(of: value))"),
                rawData: nil,
                requestID: UUID().uuidString
            )
        }
        return typed
    }
}

private struct DeduplicatorTypeMismatch: Error, LocalizedError {
    let expected: String
    let actual: String
    var errorDescription: String? {
        "Dedup type mismatch: expected \(expected), got \(actual)"
    }
}
