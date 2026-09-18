// MARK: - Circuit Breaker

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "CircuitBreaker")

/// 熔断器状态
public enum CircuitState: Sendable, Equatable {
    case closed       // 正常：允许请求
    case open         // 熔断：拒绝请求
    case halfOpen     // 半开：允许少量探测请求
}

/// 熔断器
///
/// 当连续失败达到阈值时 **自动熔断**，拒绝后续请求直到冷却期结束。
/// 冷却后进入半开状态，放行一个探测请求：成功则恢复，失败则重新熔断。
///
/// ```swift
/// let breaker = CircuitBreaker(failureThreshold: 5, resetTimeout: 30)
///
/// try breaker.preCheck()           // 检查是否允许发送
/// do {
///     let result = try await client.send(endpoint)
///     breaker.recordSuccess()      // 成功 → 重置计数
/// } catch {
///     breaker.recordFailure()      // 失败 → 累计
///     throw error
/// }
/// ```
public final class CircuitBreaker: Sendable {

    private let failureThreshold: Int
    private let resetTimeout: TimeInterval
    private let halfOpenMaxAttempts: Int

    private let state = OSAllocatedUnfairLock(initialState: MutableState())

    public init(
        failureThreshold: Int = 5,
        resetTimeout: TimeInterval = 30,
        halfOpenMaxAttempts: Int = 1
    ) {
        self.failureThreshold = failureThreshold
        self.resetTimeout = resetTimeout
        self.halfOpenMaxAttempts = halfOpenMaxAttempts
    }

    /// 当前状态
    public var currentState: CircuitState {
        state.withLock { s in
            Self.evaluateState(s, resetTimeout: resetTimeout)
        }
    }

    /// 发请求前检查：熔断中则抛错。保留已有手动编排 API。
    public func preCheck() throws {
        _ = try beginRequest()
    }

    /// NetworkClient 将许可与真实请求配对，避免旧代请求结算新一轮半开探测。
    struct RequestPermit: Sendable {
        fileprivate let generation: UInt64
        fileprivate let isHalfOpenProbe: Bool
    }

    func beginRequest() throws -> RequestPermit {
        try state.withLock { s in
            let evaluated = Self.evaluateState(s, resetTimeout: resetTimeout)
            switch evaluated {
            case .open:
                throw NetworkError.transport(
                    underlying: CircuitBreakerOpenError(),
                    requestID: UUID().uuidString
                )
            case .halfOpen:
                if s.circuitState != .halfOpen {
                    s.circuitState = .halfOpen
                    s.generation &+= 1
                    s.halfOpenAttempts = 0
                    logger.info("Circuit breaker → half-open")
                }
                guard s.halfOpenAttempts < halfOpenMaxAttempts else {
                    throw NetworkError.transport(
                        underlying: CircuitBreakerOpenError(),
                        requestID: UUID().uuidString
                    )
                }
                s.halfOpenAttempts += 1
            case .closed:
                break
            }
            return RequestPermit(
                generation: s.generation,
                isHalfOpenProbe: evaluated == .halfOpen
            )
        }
    }

    /// 记录成功：重置计数器。
    public func recordSuccess() {
        recordSuccess(matching: nil)
    }

    func recordSuccess(for permit: RequestPermit) {
        recordSuccess(matching: permit.generation)
    }

    private func recordSuccess(matching generation: UInt64?) {
        state.withLock { s in
            guard generation == nil || generation == s.generation else {
                logger.debug("Circuit breaker ignored stale success")
                return
            }
            s.consecutiveFailures = 0
            s.halfOpenAttempts = 0
            s.lastFailureTime = nil
            if s.circuitState != .closed {
                logger.info("Circuit breaker → closed")
                s.circuitState = .closed
                s.generation &+= 1
            }
        }
    }

    /// 记录失败：累加计数器
    public func recordFailure() {
        recordFailure(matching: nil)
    }

    func recordFailure(for permit: RequestPermit) {
        recordFailure(matching: permit.generation)
    }

    private func recordFailure(matching generation: UInt64?) {
        let shouldLog: (open: Bool, halfOpenFailed: Bool, count: Int) = state.withLock { s in
            guard generation == nil || generation == s.generation else {
                logger.debug("Circuit breaker ignored stale failure")
                return (false, false, s.consecutiveFailures)
            }
            s.consecutiveFailures += 1
            s.lastFailureTime = Date()
            let count = s.consecutiveFailures

            if s.consecutiveFailures >= failureThreshold && s.circuitState == .closed {
                s.circuitState = .open
                s.generation &+= 1
                return (open: true, halfOpenFailed: false, count: count)
            } else if s.circuitState == .halfOpen {
                s.circuitState = .open
                s.generation &+= 1
                s.halfOpenAttempts = 0
                return (open: false, halfOpenFailed: true, count: count)
            }
            return (open: false, halfOpenFailed: false, count: count)
        }
        if shouldLog.open {
            logger.warning("Circuit breaker → OPEN (failures: \(shouldLog.count))")
        } else if shouldLog.halfOpenFailed {
            logger.warning("Circuit breaker half-open probe failed → OPEN")
        }
    }

    /// 取消、4xx、业务与解码错误不计服务故障，但必须归还当前半开探测额度。
    /// 只释放本代真实探测，旧正常请求的取消不能放大本代的探测并发。
    func recordIgnored(for permit: RequestPermit) {
        let remaining: Int? = state.withLock { s in
            guard permit.generation == s.generation,
                  permit.isHalfOpenProbe,
                  s.circuitState == .halfOpen else { return nil }
            s.halfOpenAttempts = max(0, s.halfOpenAttempts - 1)
            return s.halfOpenAttempts
        }
        if let remaining {
            logger.info("Circuit breaker released neutral probe: remaining=\(remaining)")
        } else {
            logger.debug("Circuit breaker neutral result has no current probe to release")
        }
    }

    /// 强制重置，同时使旧请求许可失效。
    public func reset() {
        state.withLock { s in
            s = MutableState(generation: s.generation &+ 1)
        }
    }

    // MARK: - Private

    private static func evaluateState(_ s: MutableState, resetTimeout: TimeInterval) -> CircuitState {
        guard s.circuitState == .open, let lastFailure = s.lastFailureTime else {
            return s.circuitState
        }
        if Date().timeIntervalSince(lastFailure) >= resetTimeout {
            return .halfOpen
        }
        return .open
    }
}

// MARK: - Internal State

private struct MutableState: Sendable {
    var generation: UInt64 = 0
    var circuitState: CircuitState = .closed
    var consecutiveFailures: Int = 0
    var lastFailureTime: Date?
    var halfOpenAttempts: Int = 0
}

/// 熔断器打开时的错误
public struct CircuitBreakerOpenError: Error, LocalizedError, Sendable {
    public var errorDescription: String? { "Circuit breaker is open — requests are temporarily rejected" }
}
