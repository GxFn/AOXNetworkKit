// MARK: - Download Task

import Alamofire
import Foundation
import OSLog

private let logger = Logger(subsystem: "com.networkkit", category: "Download")

/// 下载进度回调
public typealias DownloadProgress = @Sendable (DownloadState) -> Void

/// 下载状态
public struct DownloadState: Sendable {
    /// 已下载字节数
    public let completedBytes: Int64
    /// 文件总字节数（未知时为 nil）
    public let totalBytes: Int64?
    /// 下载进度 0.0 ~ 1.0（总大小未知时为 nil）
    public let fractionCompleted: Double?

    public init(completedBytes: Int64, totalBytes: Int64?) {
        self.completedBytes = completedBytes
        self.totalBytes = totalBytes
        self.fractionCompleted = totalBytes.map { total in
            total > 0 ? Double(completedBytes) / Double(total) : 0
        }
    }
}

/// 下载结果
public struct DownloadResult: Sendable {
    /// 下载后的文件路径
    public let fileURL: URL
    /// 文件大小（字节）
    public let fileSize: Int64
}

/// 断点续传下载客户端
///
/// 基于 Alamofire 的 download + resumeData 实现。
/// - 通过 `SessionPool` 共享 Interceptor / SSL / EventMonitor 配置
/// - 自动管理 resumeData 持久化
/// - 支持暂停/恢复/取消
/// - 通过 `DownloadProgress` 回调实时进度
///
/// ```swift
/// let client = DownloadClient()
///
/// // 开始下载
/// let task = client.download(
///     url: videoURL,
///     to: cacheDir.appending(path: "video.mp4")
/// ) { state in
///     print("Progress: \(state.fractionCompleted ?? 0)")
/// }
///
/// // 暂停（自动保存 resumeData）
/// await task.pause()
///
/// // 恢复
/// await task.resume()
///
/// // 等待完成
/// let result = try await task.result
/// ```
public final class DownloadClient: Sendable {

    private let session: Session

    /// 使用 SessionPool 创建（推荐：共享 Interceptor/SSL/Monitor 配置）
    public init(sessionPool: SessionPool = .shared) {
        self.session = sessionPool.session(for: .download)
    }

    /// 使用自定义 Session 创建
    public init(session: Session) {
        self.session = session
    }

    /// 创建下载任务
    ///
    /// - Parameters:
    ///   - url: 远程文件 URL
    ///   - destination: 本地保存路径
    ///   - headers: 额外请求头
    ///   - progress: 进度回调（非主线程）
    /// - Returns: 可控制的下载任务句柄
    public func download(
        url: URL,
        to destination: URL,
        headers: [String: String]? = nil,
        progress: DownloadProgress? = nil
    ) -> DownloadTask {
        let resumeDataURL = Self.resumeDataURL(for: url, destination: destination)
        return DownloadTask(
            remoteURL: url,
            destination: destination,
            resumeDataURL: resumeDataURL,
            headers: headers,
            session: session,
            progress: progress
        )
    }

    // MARK: - Resume Data Path

    /// resumeData 存储路径：基于 URL + destination 的 hash
    private static func resumeDataURL(for url: URL, destination: URL) -> URL {
        let key = "\(url.absoluteString)|\(destination.path)".data(using: .utf8)!
        let hash = key.withUnsafeBytes { buffer -> String in
            var result: UInt64 = 14695981039346656037  // FNV-1a offset basis
            for byte in buffer {
                result ^= UInt64(byte)
                result &*= 1099511628211  // FNV prime
            }
            return String(result, radix: 16)
        }

        let dir = FileManager.default.temporaryDirectory.appending(path: "networkkit-downloads")
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        return dir.appending(path: "\(hash).resume")
    }
}

// MARK: - Download Task

/// 可控制的下载任务
///
/// 支持 pause / resume / cancel，暂停时自动持久化 resumeData。
public final actor DownloadTask {

    private typealias ResultContinuation = CheckedContinuation<DownloadResult, any Error>

    private let remoteURL: URL
    private let destination: URL
    private let resumeDataURL: URL
    private let headers: [String: String]?
    private let session: Session
    private let progress: DownloadProgress?

    private var downloadRequest: DownloadRequest?
    /// `result` 允许多个调用方同时等待；每个等待者都必须独立处理取消，不能互相覆盖。
    private var resultWaiters: [UUID: ResultContinuation] = [:]
    /// 下载句柄只有一个终态。缓存结果后，重复读取 `result` 必须立即返回同一结果，不能重启请求。
    private var terminalOutcome: Result<DownloadResult, any Error>?
    /// Alamofire 取消/完成回调可能晚于下一次 resume，generation 用于丢弃旧请求的回调。
    private var operationGeneration: UInt64 = 0
    private var state: TaskState = .idle

    private enum TaskState {
        case idle
        case downloading(generation: UInt64)
        case pausing(generation: UInt64, shouldResume: Bool)
        case paused
        case completed
        case cancelled
        case failed

        var isTerminal: Bool {
            switch self {
            case .completed, .cancelled, .failed:
                return true
            case .idle, .downloading, .pausing, .paused:
                return false
            }
        }
    }

    init(
        remoteURL: URL,
        destination: URL,
        resumeDataURL: URL,
        headers: [String: String]?,
        session: Session,
        progress: DownloadProgress?
    ) {
        self.remoteURL = remoteURL
        self.destination = destination
        self.resumeDataURL = resumeDataURL
        self.headers = headers
        self.session = session
        self.progress = progress
    }

    /// 等待下载完成
    public var result: DownloadResult {
        get async throws {
            try Task.checkCancellation()
            let waiterID = UUID()

            return try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation { continuation in
                    // cancellation handler 可能已排队，但 actor 尚未让出执行权；这里再检查一次，
                    // 保证“注册前取消”也不会留下永远无法恢复的 continuation。
                    guard !Task.isCancelled else {
                        continuation.resume(throwing: CancellationError())
                        return
                    }

                    if let terminalOutcome {
                        continuation.resume(with: terminalOutcome)
                        return
                    }

                    resultWaiters[waiterID] = continuation
                    if case .idle = state {
                        startNewAttempt()
                    }
                }
            } onCancel: {
                // 取消某个等待者只终止该等待；共享下载及其他等待者不受影响。
                // 如需取消整个下载，调用方应显式调用 `cancel()`。
                Task { await self.cancelWaiter(id: waiterID) }
            }
        }
    }

    /// 暂停下载（保存 resumeData 以便续传）
    public func pause() {
        guard case .downloading(let generation) = state,
              let request = downloadRequest else {
            return
        }

        // 先进入 pausing，再触发 Alamofire 取消。即使完成回调极快到达，也能识别为暂停路径。
        state = .pausing(generation: generation, shouldResume: false)
        request.cancel(producingResumeData: true)
        logger.info(
            "Download pause requested: generation=\(generation), host=\(self.remoteURL.host ?? "unknown")"
        )
    }

    /// 恢复下载
    public func resume() {
        switch state {
        case .paused:
            startNewAttempt()

        case .pausing(let generation, _):
            // resumeData 由旧请求的完成回调产生。立即 resume 时先记下意图，等保存完成后再启动，
            // 避免旧取消回调误伤新请求，也避免在 resumeData 尚未落盘时从零重复下载。
            state = .pausing(generation: generation, shouldResume: true)
            logger.info(
                "Download resume queued while pausing: generation=\(generation), host=\(self.remoteURL.host ?? "unknown")"
            )

        case .idle, .downloading, .completed, .cancelled, .failed:
            return
        }
    }

    /// 取消下载（清理 resumeData）
    public func cancel() {
        guard !state.isTerminal else { return }

        // 先递增 generation 并清空当前请求，再触发取消；晚到回调只会命中 stale 分支。
        operationGeneration &+= 1
        let request = downloadRequest
        downloadRequest = nil
        state = .cancelled
        request?.cancel()
        cleanResumeData()
        finish(
            with: .failure(CancellationError()),
            terminalState: .cancelled
        )
        logger.info("Download cancelled: host=\(self.remoteURL.host ?? "unknown")")
    }

    // MARK: - Internal

    private func startNewAttempt() {
        guard terminalOutcome == nil else { return }

        operationGeneration &+= 1
        let generation = operationGeneration
        state = .downloading(generation: generation)

        let afDestination: DownloadRequest.Destination = { [destination] _, _ in
            (destination, [.removePreviousFile, .createIntermediateDirectories])
        }

        let request: DownloadRequest

        // 检查是否有 resumeData
        if let resumeData = loadResumeData() {
            request = session.download(resumingWith: resumeData, to: afDestination)
                .validate(statusCode: 200..<300)
            logger.info(
                "Download resuming: generation=\(generation), host=\(self.remoteURL.host ?? "unknown"), resumeBytes=\(resumeData.count)"
            )
        } else {
            var httpHeaders: HTTPHeaders?
            if let headers {
                httpHeaders = HTTPHeaders(headers.map { HTTPHeader(name: $0.key, value: $0.value) })
            }
            request = session.download(remoteURL, headers: httpHeaders, to: afDestination)
                .validate(statusCode: 200..<300)
            logger.info(
                "Download starting: generation=\(generation), host=\(self.remoteURL.host ?? "unknown")"
            )
        }

        // 进度回调也校验 generation，防止 pause/resume 后旧请求的进度覆盖新请求。
        if let progress {
            request.downloadProgress { [weak self] value in
                let completedBytes = value.completedUnitCount
                let totalBytes = value.totalUnitCount > 0 ? value.totalUnitCount : nil
                Task { [weak self] in
                    await self?.handleProgress(
                        completedBytes: completedBytes,
                        totalBytes: totalBytes,
                        generation: generation,
                        callback: progress
                    )
                }
            }
        }

        // 完成回调
        request.response { [weak self] response in
            guard let self else { return }
            Task { await self.handleCompletion(response, generation: generation) }
        }

        self.downloadRequest = request
    }

    private func handleProgress(
        completedBytes: Int64,
        totalBytes: Int64?,
        generation: UInt64,
        callback: DownloadProgress
    ) {
        guard case .downloading(let activeGeneration) = state,
              activeGeneration == generation else {
            return
        }

        callback(DownloadState(completedBytes: completedBytes, totalBytes: totalBytes))
    }

    private func handleCompletion(
        _ response: AFDownloadResponse<URL?>,
        generation: UInt64
    ) {
        switch state {
        case .downloading(let activeGeneration) where activeGeneration == generation:
            downloadRequest = nil
            handleActiveCompletion(response, generation: generation)

        case .pausing(let activeGeneration, let shouldResume) where activeGeneration == generation:
            downloadRequest = nil
            handlePauseCompletion(
                response,
                generation: generation,
                shouldResume: shouldResume
            )

        case .idle, .paused, .completed, .cancelled, .failed, .downloading, .pausing:
            logger.debug(
                "Download ignored stale completion: generation=\(generation), current=\(self.operationGeneration), host=\(self.remoteURL.host ?? "unknown")"
            )
        }
    }

    private func handleActiveCompletion(
        _ response: AFDownloadResponse<URL?>,
        generation: UInt64
    ) {
        if let error = response.error {
            let networkError = NetworkError.from(
                afError: error,
                response: response.response,
                data: nil,
                requestID: UUID().uuidString
            )
            finish(with: .failure(networkError), terminalState: .failed)
            // 不记录 URL、目标路径、请求头或错误正文，避免日志泄漏用户内容和鉴权信息。
            logger.error(
                "Download failed: generation=\(generation), host=\(self.remoteURL.host ?? "unknown"), category=transport"
            )
            return
        }

        guard let fileURL = response.fileURL else {
            let error = NetworkError.transport(
                underlying: URLError(.cannotCreateFile),
                requestID: UUID().uuidString
            )
            finish(with: .failure(error), terminalState: .failed)
            logger.error(
                "Download failed without destination file: generation=\(generation), host=\(self.remoteURL.host ?? "unknown")"
            )
            return
        }

        complete(fileURL: fileURL, generation: generation)
    }

    private func handlePauseCompletion(
        _ response: AFDownloadResponse<URL?>,
        generation: UInt64,
        shouldResume: Bool
    ) {
        // 极小文件可能在 cancel 生效前已经完成；成功结果优先，不能退回 paused 后重复下载。
        if response.error == nil, let fileURL = response.fileURL {
            complete(fileURL: fileURL, generation: generation)
            return
        }

        if let resumeData = response.resumeData {
            saveResumeData(resumeData)
        }

        if let error = response.error,
           !error.isExplicitlyCancelledError,
           response.resumeData == nil {
            let networkError = NetworkError.from(
                afError: error,
                response: response.response,
                data: nil,
                requestID: UUID().uuidString
            )
            finish(with: .failure(networkError), terminalState: .failed)
            logger.error(
                "Download failed while pausing: generation=\(generation), host=\(self.remoteURL.host ?? "unknown"), category=transport"
            )
            return
        }

        state = .paused
        logger.info(
            "Download paused: generation=\(generation), host=\(self.remoteURL.host ?? "unknown"), hasResumeData=\(response.resumeData != nil)"
        )
        if shouldResume {
            startNewAttempt()
        }
    }

    private func complete(fileURL: URL, generation: UInt64) {
        cleanResumeData()
        let attributes = try? FileManager.default.attributesOfItem(atPath: fileURL.path)
        let fileSize = (attributes?[.size] as? NSNumber)?.int64Value ?? 0
        let result = DownloadResult(fileURL: fileURL, fileSize: fileSize)
        finish(with: .success(result), terminalState: .completed)
        logger.info(
            "Download complete: generation=\(generation), host=\(self.remoteURL.host ?? "unknown"), bytes=\(fileSize)"
        )
    }

    private func finish(
        with outcome: Result<DownloadResult, any Error>,
        terminalState: TaskState
    ) {
        guard terminalOutcome == nil else { return }
        terminalOutcome = outcome
        state = terminalState
        downloadRequest = nil

        // 先复制再清空字典，避免迭代一个已被修改的 Dictionary.Values 视图。
        let waiters = Array(resultWaiters.values)
        resultWaiters.removeAll()
        for waiter in waiters {
            waiter.resume(with: outcome)
        }
    }

    private func cancelWaiter(id: UUID) {
        guard let waiter = resultWaiters.removeValue(forKey: id) else { return }
        waiter.resume(throwing: CancellationError())
        logger.debug(
            "Download waiter cancelled: remaining=\(self.resultWaiters.count), host=\(self.remoteURL.host ?? "unknown")"
        )
    }

    // MARK: - Resume Data Persistence

    private func saveResumeData(_ data: Data) {
        do {
            try data.write(to: resumeDataURL, options: .atomic)
            logger.debug("Resume data saved: \(data.count) bytes")
        } catch {
            logger.warning("Failed to save resume data: category=file-system")
        }
    }

    private func loadResumeData() -> Data? {
        guard FileManager.default.fileExists(atPath: resumeDataURL.path) else { return nil }
        return try? Data(contentsOf: resumeDataURL)
    }

    private func cleanResumeData() {
        try? FileManager.default.removeItem(at: resumeDataURL)
    }
}
