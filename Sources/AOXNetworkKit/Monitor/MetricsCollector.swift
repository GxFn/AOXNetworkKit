// MARK: - Metrics Collector

import Foundation
import os

private let logger = Logger(subsystem: "com.networkkit", category: "Metrics")

/// 单次请求指标
public struct RequestMetrics: Sendable {
    public let path: String
    public let method: String
    public let statusCode: Int?
    public let duration: TimeInterval
    public let bytesSent: Int64
    public let bytesReceived: Int64
    public let succeeded: Bool
    public let errorType: String?
    public let timestamp: Date

    public init(
        path: String,
        method: String,
        statusCode: Int? = nil,
        duration: TimeInterval,
        bytesSent: Int64 = 0,
        bytesReceived: Int64 = 0,
        succeeded: Bool,
        errorType: String? = nil,
        timestamp: Date = Date()
    ) {
        self.path = path
        self.method = method
        self.statusCode = statusCode
        self.duration = duration
        self.bytesSent = bytesSent
        self.bytesReceived = bytesReceived
        self.succeeded = succeeded
        self.errorType = errorType
        self.timestamp = timestamp
    }
}

/// 聚合统计
public struct AggregatedStats: Sendable {
    public let totalRequests: Int
    public let successCount: Int
    public let failureCount: Int
    public let averageDuration: TimeInterval
    public let p95Duration: TimeInterval
    public let totalBytesSent: Int64
    public let totalBytesReceived: Int64

    public var successRate: Double {
        totalRequests > 0 ? Double(successCount) / Double(totalRequests) : 0
    }
}

/// 指标收集器回调
public protocol MetricsDelegate: AnyObject, Sendable {
    func metricsCollector(_ collector: MetricsCollector, didRecord metrics: RequestMetrics)
}

/// 网络请求指标收集器
///
/// 收集请求耗时、成功率、字节流量等指标。
/// 保存最近 N 条记录用于本地统计，支持 delegate 回调用于外部上报。
///
/// ```swift
/// let metrics = MetricsCollector(maxHistory: 200)
///
/// // 记录一次成功请求
/// metrics.record(RequestMetrics(
///     path: "/x/web-interface/popular",
///     method: "GET",
///     statusCode: 200,
///     duration: 0.35,
///     bytesReceived: 4096,
///     succeeded: true
/// ))
///
/// // 查看聚合统计
/// let stats = metrics.aggregatedStats()
/// print("Success rate: \(stats.successRate)")
/// ```
public final class MetricsCollector: Sendable {

    /// 锁只保护盒子内部的 weak 引用；不能把 existential 直接放进锁状态，
    /// 否则公开属性即使写成 `weak`，实际仍会被锁内 Optional 强持有。
    private final class WeakDelegateBox: @unchecked Sendable {
        weak var value: (any MetricsDelegate)?
    }

    private let maxHistory: Int
    private let state: OSAllocatedUnfairLock<[RequestMetrics]>

    public var delegate: (any MetricsDelegate)? {
        get { delegateState.withLock { $0.value } }
        set { delegateState.withLock { $0.value = newValue } }
    }
    private let delegateState = OSAllocatedUnfairLock<WeakDelegateBox>(initialState: WeakDelegateBox())

    public init(maxHistory: Int = 500) {
        // 外部配置可能来自远端或 UserDefaults；负数不能传入 removeFirst 形成越界崩溃。
        self.maxHistory = max(0, maxHistory)
        self.state = OSAllocatedUnfairLock(initialState: [])
    }

    /// 记录一次请求指标
    public func record(_ metrics: RequestMetrics) {
        state.withLock { history in
            guard self.maxHistory > 0 else { return }
            history.append(metrics)
            if history.count > self.maxHistory {
                history.removeFirst(history.count - self.maxHistory)
            }
        }
        delegate?.metricsCollector(self, didRecord: metrics)

        if !metrics.succeeded {
            // path 可能包含房间号、用户 id 等业务标识；日志只保留方法和稳定错误分类。
            logger.debug(
                "Request failed: method=\(metrics.method), category=\(metrics.errorType ?? "unknown")"
            )
        }
    }

    /// 获取聚合统计
    public func aggregatedStats() -> AggregatedStats {
        let history = state.withLock { $0 }
        guard !history.isEmpty else {
            return AggregatedStats(
                totalRequests: 0, successCount: 0, failureCount: 0,
                averageDuration: 0, p95Duration: 0,
                totalBytesSent: 0, totalBytesReceived: 0
            )
        }

        let successes = history.filter(\.succeeded).count
        let failures = history.count - successes
        let totalDuration = history.reduce(0.0) { $0 + $1.duration }
        let avgDuration = totalDuration / Double(history.count)

        // P95
        let sorted = history.map(\.duration).sorted()
        let p95Index = Int(Double(sorted.count) * 0.95)
        let p95 = sorted[min(p95Index, sorted.count - 1)]

        // 单个 URLSession task 已做饱和求和；历史聚合仍必须再次防溢出，
        // 否则多个大请求会在 Debug 下 trap、Release 下回绕成负流量。
        let bytesSent = Self.saturatingNonnegativeSum(history.map(\.bytesSent))
        let bytesReceived = Self.saturatingNonnegativeSum(history.map(\.bytesReceived))

        return AggregatedStats(
            totalRequests: history.count,
            successCount: successes,
            failureCount: failures,
            averageDuration: avgDuration,
            p95Duration: p95,
            totalBytesSent: bytesSent,
            totalBytesReceived: bytesReceived
        )
    }

    /// 获取指定路径的请求历史
    public func history(for path: String) -> [RequestMetrics] {
        state.withLock { $0.filter { $0.path == path } }
    }

    /// 清空历史
    public func reset() {
        state.withLock { $0.removeAll() }
    }

    private static func saturatingNonnegativeSum(_ values: [Int64]) -> Int64 {
        values.reduce(into: Int64(0)) { total, value in
            let nonnegativeValue = max(0, value)
            let (sum, overflow) = total.addingReportingOverflow(nonnegativeValue)
            total = overflow ? .max : sum
        }
    }
}
