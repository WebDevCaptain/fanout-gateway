# MarketMux Implementation Plan: End-to-End Slice Approach

## Objective
Build the Market Data Distribution Pipeline iteratively by establishing a full end-to-end data flow first. We will start with a mock publisher sending data to Redis, consume it with a simple subscriber, and progressively evolve that subscriber into the full, highly-concurrent WebSocket Edge Gateway.

## Phase 1: Basic Ingestion & Redis Backplane
**Goal:** Establish the foundational data flow without worrying about WebSockets or concurrency yet.

1. **Project Setup:**
   - Initialize the directory structure (`cmd/`, `internal/`).
   - Install required dependencies (`go get github.com/gin-gonic/gin`, `github.com/redis/go-redis/v9`, `github.com/gorilla/websocket`).

2. **The Mock Publisher (`cmd/mock-publisher/main.go`):**
   - Create a standalone Go process with a `time.Ticker`.
   - Generate randomized JSON payloads `{"symbol": "AAPL", "price": 150.25, "timestamp": ...}` for a set of symbols.
   - Connect to a local Redis instance and publish to a Pub/Sub channel (e.g., `market.ticks`).

3. **The Dummy Subscriber (`cmd/gateway/main.go`):**
   - Connect to the same Redis instance.
   - Spawn a background Goroutine that subscribes to `market.ticks` and simply logs the received JSON to stdout. This proves Zone 1 to Zone 3 communication.

## Phase 2: The Core API & Subscription Hub
**Goal:** Build the thread-safe core of the Edge Gateway to route messages.

1. **Gin Router Setup:**
   - Initialize a Gin HTTP server in `cmd/gateway/main.go`.
   - Implement `POST /api/v1/ticks` to allow external scripts to ingest data directly via HTTP (which then publishes to Redis).

2. **Thread-Safe Hub (`internal/hub/hub.go`):**
   - Define the `Hub` struct with a `sync.RWMutex`.
   - Implement the core state: `map[string]map[*Client]bool` (mapping symbols like "AAPL" to connected clients).
   - Implement safe `Subscribe(client, symbol)`, `Unsubscribe(client, symbol)`, and `Broadcast(symbol, payload)` methods.

3. **Connecting Redis to the Hub:**
   - Update the Redis Listener from Phase 1 to call `hub.Broadcast()` instead of just logging to stdout.

## Phase 3: WebSocket Upgrade & Client Fan-Out
**Goal:** Connect real users to the Hub and test the massive concurrency fan-out.

1. **The Client Struct (`internal/ws/client.go`):**
   - Define the `Client` struct holding the WebSocket connection, a reference to the Hub, and a `send` channel (`chan []byte`).

2. **The Gorilla Upgrader:**
   - Implement the `GET /ws` endpoint in Gin.
   - Hijack the connection and instantiate a new `Client`.

3. **The Goroutine Pumps:**
   - Implement `writePump`: Listens to the `send` channel and writes to the WebSocket.
   - Implement `readPump`: Listens to the WebSocket for incoming messages (like `{"action": "subscribe", "symbol": "AAPL"}`), calls `hub.Subscribe()`, and handles client disconnects safely.

## Phase 4: Testing, Load Generation, & Profiling
**Goal:** Prove the system handles Lock Contention and Goroutine Leaks flawlessly.

1. **Race Detection Tests (`internal/hub/hub_test.go`):**
   - Write unit tests using `net/http/httptest`.
   - Spawn 100 concurrent goroutines rapidly subscribing, unsubscribing, and broadcasting to trigger potential race conditions.

2. **The Load Tester (`cmd/loadtest/main.go`):**
   - Build a standalone script using `sync.WaitGroup` to open 10,000 WebSocket connections to the local gateway.
   - Randomly subscribe to stocks and measure message throughput.

3. **Performance Profiling:**
   - Integrate `_ "net/http/pprof"` into the Gin router.
   - Run the load test and monitor `/debug/pprof/goroutine` to ensure the goroutine count drops back to baseline when the 10,000 clients disconnect (verifying zero goroutine leaks).

## Verification & Iteration
After each phase, we will manually test the components (e.g., verifying Redis logs, connecting via a simple HTML WebSocket client) before proceeding. We will strictly adhere to the project's requirement of explicit, idiomatic Go concurrency handling.