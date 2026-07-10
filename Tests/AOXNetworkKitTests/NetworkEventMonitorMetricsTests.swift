import Alamofire
import Foundation
import XCTest
@testable import AOXNetworkKit

final class NetworkEventMonitorMetricsTests: XCTestCase {
    func testMonitorRecordsOnlyOnceWhenOneDataRequestHasMultipleSerializers() {
        let collector = MetricsCollector()
        let monitor = NetworkEventMonitor(metricsCollector: collector)
        let session = Session(
            configuration: .ephemeral,
            startRequestsImmediately: false
        )
        let url = URL(string: "https://metrics.unit.test/resource")!
        let urlRequest = URLRequest(url: url)
        let dataRequest = session.request(urlRequest)
        let httpResponse = HTTPURLResponse(
            url: url,
            statusCode: 200,
            httpVersion: "HTTP/1.1",
            headerFields: ["Content-Length": "2"]
        )!
        let response = DataResponse<Data, AFError>(
            request: urlRequest,
            response: httpResponse,
            data: Data("ok".utf8),
            metrics: nil,
            serializationDuration: 0.01,
            result: .success(Data("ok".utf8))
        )

        monitor.request(dataRequest, didParseResponse: response)
        monitor.request(dataRequest, didParseResponse: response)

        let stats = collector.aggregatedStats()
        XCTAssertEqual(stats.totalRequests, 1)
        XCTAssertEqual(stats.successCount, 1)
        dataRequest.cancel()
        session.cancelAllRequests()
    }

    func testBuilderAggregatesRedirectTransactionsAndUsesFinalResponseStatus() {
        let metrics = NetworkRequestMetricsBuilder.make(
            path: "/resource",
            method: "POST",
            responseStatusCode: 204,
            taskDuration: 1.25,
            transactions: [
                NetworkTransactionMetric(statusCode: 302, bytesSent: 10, bytesReceived: 20),
                NetworkTransactionMetric(statusCode: 200, bytesSent: 30, bytesReceived: 40),
            ],
            errorType: nil
        )

        XCTAssertEqual(metrics.statusCode, 204, "DataResponse 的最终状态码优先于重定向 transaction")
        XCTAssertEqual(metrics.duration, 1.25, accuracy: 0.0001)
        XCTAssertEqual(metrics.bytesSent, 40)
        XCTAssertEqual(metrics.bytesReceived, 60)
        XCTAssertTrue(metrics.succeeded)
        XCTAssertNil(metrics.errorType)
    }

    func testBuilderRecordsSerializationFailureEvenForHTTP200() {
        let metrics = NetworkRequestMetricsBuilder.make(
            path: "/payload",
            method: "GET",
            responseStatusCode: 200,
            taskDuration: 0.2,
            transactions: [],
            errorType: "responseSerializationFailed"
        )

        XCTAssertFalse(metrics.succeeded)
        XCTAssertEqual(metrics.errorType, "responseSerializationFailed")
    }

    func testBuilderUsesTransactionStatusAndClassifiesUnvalidatedHTTPFailure() {
        let metrics = NetworkRequestMetricsBuilder.make(
            path: "/unavailable",
            method: "GET",
            responseStatusCode: nil,
            taskDuration: 0.4,
            transactions: [
                NetworkTransactionMetric(statusCode: 503, bytesSent: 0, bytesReceived: 12),
            ],
            errorType: nil
        )

        XCTAssertEqual(metrics.statusCode, 503)
        XCTAssertFalse(metrics.succeeded)
        XCTAssertEqual(metrics.errorType, "HTTPStatus")
    }

    func testSummaryClampsUnknownValuesAndSaturatesOverflow() {
        let summary = NetworkRequestMetricsBuilder.summarize(
            taskDuration: -1,
            transactions: [
                NetworkTransactionMetric(
                    statusCode: 301,
                    bytesSent: -1,
                    bytesReceived: Int64.max
                ),
                NetworkTransactionMetric(
                    statusCode: 200,
                    bytesSent: 5,
                    bytesReceived: 10
                ),
            ]
        )

        XCTAssertEqual(summary.duration, 0)
        XCTAssertEqual(summary.bytesSent, 5)
        XCTAssertEqual(summary.bytesReceived, Int64.max)
        XCTAssertEqual(summary.finalStatusCode, 200)
    }

    func testLogicalRequestGateAllowsExactlyOneConcurrentMetric() async {
        let gate = LogicalRequestMetricGate()
        let token = MetricGateToken()

        let claims = await withTaskGroup(of: Bool.self, returning: [Bool].self) { group in
            for _ in 0..<64 {
                group.addTask {
                    gate.claim(token)
                }
            }

            var claims: [Bool] = []
            for await claim in group {
                claims.append(claim)
            }
            return claims
        }

        XCTAssertEqual(claims.filter { $0 }.count, 1)
    }

    func testCollectorDelegateIsActuallyWeak() {
        let collector = MetricsCollector()
        weak var releasedDelegate: RecordingMetricsDelegate?

        do {
            let delegate = RecordingMetricsDelegate()
            releasedDelegate = delegate
            collector.delegate = delegate
            XCTAssertNotNil(collector.delegate)
        }

        XCTAssertNil(releasedDelegate)
        XCTAssertNil(collector.delegate)
    }

    func testCollectorClampsHistoryConfigurationAndAggregateOverflow() {
        let disabledCollector = MetricsCollector(maxHistory: -1)
        disabledCollector.record(metric(bytesSent: 1, bytesReceived: 1))
        XCTAssertEqual(disabledCollector.aggregatedStats().totalRequests, 0)

        let collector = MetricsCollector(maxHistory: 2)
        collector.record(metric(bytesSent: .max, bytesReceived: .max))
        collector.record(metric(bytesSent: 10, bytesReceived: 20))

        let stats = collector.aggregatedStats()
        XCTAssertEqual(stats.totalRequests, 2)
        XCTAssertEqual(stats.totalBytesSent, .max)
        XCTAssertEqual(stats.totalBytesReceived, .max)
    }

    private func metric(bytesSent: Int64, bytesReceived: Int64) -> RequestMetrics {
        RequestMetrics(
            path: "/aggregate",
            method: "GET",
            duration: 0.1,
            bytesSent: bytesSent,
            bytesReceived: bytesReceived,
            succeeded: true
        )
    }
}

private final class MetricGateToken: @unchecked Sendable {}

private final class RecordingMetricsDelegate: MetricsDelegate, @unchecked Sendable {
    func metricsCollector(_ collector: MetricsCollector, didRecord metrics: RequestMetrics) {}
}
