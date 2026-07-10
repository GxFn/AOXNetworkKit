// MARK: - Network Event Monitor

import Alamofire
import Foundation
import OSLog
import os

private let logger = Logger(subsystem: "com.networkkit", category: "Network")

/// 基于 Alamofire EventMonitor 的请求生命周期监控
///
/// 替代手动日志中间件，利用 Alamofire 提供的完整生命周期回调：
/// - 请求开始/完成的结构化日志
/// - 自动采集 URLSessionTaskMetrics（DNS/TLS/连接/传输各阶段耗时）
/// - 向 MetricsCollector 上报聚合指标
public final class NetworkEventMonitor: EventMonitor {

    public let queue = DispatchQueue(label: "com.networkkit.event-monitor", qos: .utility)

    private let metricsCollector: MetricsCollector?
    /// 同一 DataRequest 可以注册多个 response serializer；每个 serializer 都会触发
    /// `didParseResponse`。弱引用 gate 保证一次逻辑请求只上报一次且不会永久持有 Request。
    private let metricGate = LogicalRequestMetricGate()

    public init(metricsCollector: MetricsCollector? = nil) {
        self.metricsCollector = metricsCollector
    }

    // MARK: - Request Lifecycle

    public func requestDidResume(_ request: Request) {
        let method = request.request?.httpMethod ?? "?"
        let host = request.request?.url?.host ?? "unknown"
        logger.debug("Network request resumed: method=\(method), host=\(host)")
    }

    public func request(_ request: Request, didValidateRequest urlRequest: URLRequest?, response: HTTPURLResponse, data: Data?, withResult result: Request.ValidationResult) {
        switch result {
        case .success:
            break
        case .failure(let error):
            let host = urlRequest?.url?.host ?? "unknown"
            logger.warning(
                "Network validation failed: host=\(host), category=\(String(describing: type(of: error)))"
            )
        }
    }

    // MARK: - Task Metrics (DNS/TLS/Transfer timing)

    public func request(_ request: Request, didGatherMetrics metrics: URLSessionTaskMetrics) {
        guard let transaction = metrics.transactionMetrics.last,
              let urlRequest = request.request else {
            return
        }

        // 这里只保留 URLSession 分阶段诊断，不向 MetricsCollector 记账。
        // 同一个逻辑请求可能经历重试/重定向并产生多个 task metrics，若在这里记录会双计数。
        let host = urlRequest.url?.host ?? "unknown"
        let dnsMs = Self.phaseDuration(
            from: transaction.domainLookupStartDate,
            to: transaction.domainLookupEndDate
        )
        let tlsMs = Self.phaseDuration(
            from: transaction.secureConnectionStartDate,
            to: transaction.secureConnectionEndDate
        )
        if dnsMs != nil || tlsMs != nil {
            let dnsText = dnsMs.map { String(format: "%.0f", $0) } ?? "n/a"
            let tlsText = tlsMs.map { String(format: "%.0f", $0) } ?? "n/a"
            logger.debug(
                "Network phases: host=\(host), dnsMs=\(dnsText), tlsMs=\(tlsText)"
            )
        }
    }

    // MARK: - Logical Response Metric

    public func request<Value>(_ request: DataRequest, didParseResponse response: DataResponse<Value, AFError>) {
        // DataRequest 允许多个 response handler；只让第一个 parse 结果代表本次逻辑请求。
        guard metricGate.claim(request) else {
            logger.debug("Network duplicate response metric ignored")
            return
        }

        let path = request.request?.url?.path ?? "unknown"
        let method = request.request?.httpMethod ?? "GET"
        let taskMetrics = response.metrics
        let transactions = taskMetrics?.transactionMetrics.map { transaction in
            NetworkTransactionMetric(
                statusCode: (transaction.response as? HTTPURLResponse)?.statusCode,
                bytesSent: transaction.countOfRequestBodyBytesSent,
                bytesReceived: transaction.countOfResponseBodyBytesReceived
            )
        } ?? []
        let errorType = response.error.map { String(describing: type(of: $0)) }

        let requestMetrics = NetworkRequestMetricsBuilder.make(
            path: path,
            method: method,
            responseStatusCode: response.response?.statusCode,
            taskDuration: taskMetrics?.taskInterval.duration ?? 0,
            transactions: transactions,
            errorType: errorType
        )
        metricsCollector?.record(requestMetrics)

        if response.error != nil {
            let host = request.request?.url?.host ?? "unknown"
            logger.warning(
                "Network response failed: method=\(method), host=\(host), category=\(errorType ?? "transport")"
            )
        }
    }

    private static func phaseDuration(from start: Date?, to end: Date?) -> Double? {
        guard let start, let end else { return nil }
        return max(0, end.timeIntervalSince(start) * 1000)
    }
}

// MARK: - Deterministic Metrics Helpers

/// URLSession transaction 中与聚合相关的最小快照，避免测试依赖不可直接构造的
/// `URLSessionTaskTransactionMetrics`。
struct NetworkTransactionMetric: Sendable, Equatable {
    let statusCode: Int?
    let bytesSent: Int64
    let bytesReceived: Int64
}

struct NetworkTransferMetric: Sendable, Equatable {
    let duration: TimeInterval
    let bytesSent: Int64
    let bytesReceived: Int64
    let finalStatusCode: Int?
}

enum NetworkRequestMetricsBuilder {
    static func summarize(
        taskDuration: TimeInterval,
        transactions: [NetworkTransactionMetric]
    ) -> NetworkTransferMetric {
        NetworkTransferMetric(
            duration: max(0, taskDuration),
            bytesSent: saturatingNonnegativeSum(transactions.map(\.bytesSent)),
            bytesReceived: saturatingNonnegativeSum(transactions.map(\.bytesReceived)),
            finalStatusCode: transactions.last?.statusCode
        )
    }

    static func make(
        path: String,
        method: String,
        responseStatusCode: Int?,
        taskDuration: TimeInterval,
        transactions: [NetworkTransactionMetric],
        errorType: String?
    ) -> RequestMetrics {
        let transfer = summarize(taskDuration: taskDuration, transactions: transactions)
        let statusCode = responseStatusCode ?? transfer.finalStatusCode
        let statusSucceeded = statusCode.map { (200..<300).contains($0) } ?? true
        let succeeded = errorType == nil && statusSucceeded
        let resolvedErrorType = errorType ?? (statusSucceeded ? nil : "HTTPStatus")

        return RequestMetrics(
            path: path,
            method: method,
            statusCode: statusCode,
            duration: transfer.duration,
            bytesSent: transfer.bytesSent,
            bytesReceived: transfer.bytesReceived,
            succeeded: succeeded,
            errorType: resolvedErrorType
        )
    }

    private static func saturatingNonnegativeSum(_ values: [Int64]) -> Int64 {
        values.reduce(into: Int64(0)) { total, value in
            let nonnegativeValue = max(0, value)
            let (sum, overflow) = total.addingReportingOverflow(nonnegativeValue)
            total = overflow ? .max : sum
        }
    }
}

/// 以对象身份为 key 的弱引用一次性门禁。Request 释放后会在下一次 claim 时清理，
/// 不延长网络请求生命周期，也不会让长时间运行的 App 持续积累已完成请求。
final class LogicalRequestMetricGate: Sendable {
    private final class WeakBox: @unchecked Sendable {
        weak var value: AnyObject?

        init(_ value: AnyObject) {
            self.value = value
        }
    }

    private let state = OSAllocatedUnfairLock<[ObjectIdentifier: WeakBox]>(initialState: [:])

    func claim(_ request: AnyObject) -> Bool {
        let identifier = ObjectIdentifier(request)
        // 先在锁闭包外包装为明确的 @unchecked Sendable 弱引用盒，避免把任意非 Sendable
        // Foundation/Alamofire 对象直接跨进 OSAllocatedUnfairLock 的 @Sendable 闭包。
        let weakBox = WeakBox(request)
        return state.withLock { entries in
            entries = entries.filter { $0.value.value != nil }
            guard entries[identifier] == nil else { return false }
            entries[identifier] = weakBox
            return true
        }
    }
}
