# MarketMux (fanout-gateway) - Project Context

## Project Overview
MarketMux is a high-performance market data distribution pipeline designed for low-latency fan-out of real-time price ticks to numerous WebSocket clients. The project is being built iteratively, following an "end-to-end slice" approach.

### Architecture
1.  **Mock Publisher (`cmd/mock-publisher`):** Simulates real-time market data (e.g., AAPL, GOOGL) and publishes it to a Redis Pub/Sub channel.
2.  **Redis/Valkey Backplane:** Acts as the message broker between publishers and the gateway.
3.  **Edge Gateway (`cmd/gateway`):** (In Progress) Subscribes to Redis and will eventually handle WebSocket client connections, managing subscriptions and fanning out data.

## Key Technologies
- **Language:** Go (v1.26+)
- **Message Broker:** Redis / Valkey (Pub/Sub)
- **Web Framework:** Gin (Planned)
- **WebSocket:** Gorilla WebSocket (Planned)
- **Containerization:** Docker Compose (for Valkey)

## Building and Running

### Prerequisites
- Go 1.26 or higher
- Docker and Docker Compose

### Commands
- **Start Infrastructure:**
  ```bash
  docker-compose up -d
  ```
- **Run Mock Publisher:**
  ```bash
  go run cmd/mock-publisher/main.go
  ```
- **Run Edge Gateway (Dummy Subscriber):**
  ```bash
  go run cmd/gateway/main.go
  ```

## Development Conventions
- **Concurrency:** Prefer explicit, idiomatic Go concurrency patterns. Use `sync.RWMutex` for thread-safe state management in the hub.
- **Testing:** New features should include race detection tests. Use `go test -race ./...` for validation.
- **Structure:**
    - `cmd/`: Entry points for applications.
    - `internal/`: Private library code, including models and business logic.
    - `initial-plan.md`: The roadmap for project evolution.

## Current Status: Phase 3
The core fan-out pipeline is now complete. The gateway successfully upgrades HTTP connections to WebSockets, allowing clients to subscribe to specific symbols. Market data is consumed from Redis and efficiently broadcast to all subscribed WebSocket clients in real-time. Next steps involve building a frontend visualization and adding production-grade features like authentication.
