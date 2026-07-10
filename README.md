# AOXNetworkKit

基于 Alamofire 的轻量级类型安全网络层，零业务耦合。

## 特性

- **类型安全 Endpoint** — 泛型绑定请求/响应类型，编译期检查
- **中间件链** — 可插拔的 `Middleware` 协议（签名、认证、重试、日志）
- **Multi-Session 连接池** — 按优先级分流到独立 URLSession（standard / realtime / prefetch）
- **统一错误类型** — `NetworkError` 区分可重试/不可重试，携带 requestID 日志追踪
- **协议抽象** — `RequestSigner`、`ResponseValidatable`、`NetworkClientProtocol` 全面可 Mock
- **可靠下载状态机** — 多等待者、pause/resume 代次、显式取消与终态缓存
- **WebSocket 诊断** — 握手后连接态、有界缓冲、远端 close code/reason 和过期回调隔离
- **单次指标记账** — 每个 DataRequest 只生成一个成功/失败指标，避免多 serializer 重复统计

## 要求

- iOS 16+
- Swift 6.0
- Alamofire 5.9+

## 安装

### Swift Package Manager

```swift
.package(url: "https://github.com/GxFn/AOXNetworkKit.git", from: "0.1.0")
```

## 快速使用

```swift
import AOXNetworkKit

// 1. 定义端点
extension Endpoint where Response == MyResponse {
    static func fetchItems(page: Int) -> Endpoint {
        Endpoint(path: "/api/items", parameters: ["page": page])
    }
}

// 2. 发送请求
let client = NetworkClient(
    defaultBaseURL: "https://api.example.com",
    middlewares: [RetryMiddleware(), LogMiddleware()]
)
let response = try await client.send(.fetchItems(page: 1))
```

## 验证

仓库提交 `Package.resolved`，CI 只使用锁定的 Alamofire revision：

```bash
xcodebuild \
  -scheme AOXNetworkKit \
  -destination 'platform=iOS Simulator,name=iPhone 16 Pro,OS=latest' \
  -onlyUsePackageVersionsFromResolvedFile \
  CODE_SIGNING_ALLOWED=NO \
  test
```

## 架构

```
AOXNetworkKit/
├── Core/               — Endpoint, HTTPMethod, NetworkError, ResponseDecoder
├── Client/             — NetworkClient, DownloadClient, UploadClient, WebSocketClient
├── Session/            — SessionPool, RetryPolicy, SSLPinning, RequestSigner
├── Middleware/          — 可插拔中间件链
├── Monitor/            — MetricsCollector, NetworkEventMonitor
├── Resilience/         — CircuitBreaker, RequestDeduplicator
└── Testing/            — MockClient
```

## License

MIT
