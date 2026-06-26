# Mini Exchange Portfolio System (Kafka & Binance API Integrated)

A simplified trading platform backend consisting of 3 microservices, built with **Rust** and using **Kafka** for event-driven price updates and audit logging, plus **Binance WebSocket API** for real-time cryptocurrency prices.

## Architecture

```
                                  ┌────────────────────┐
                                  │   Binance API      │
                                  └─────────┬──────────┘
                                            │ (WebSocket Stream)
                                            ▼
┌──────────────────┐  Stream Price  ┌───────────────┐
│   Portfolio      │◄───────────────│    Market     │
│   Service        │   (Kafka)      │    Service    │
│   :8082          │                │    :8081      │
└────────┬─────────┘                └───────────────┘
         │
         │ Stream Audit Events (Kafka)
         ▼
┌──────────────────┐
│    Audit         │
│    Service       │
│    :8083         │
└──────────────────┘
```

### Event-Driven Communication (via Kafka)
1. **Price Streaming**: `Market Service` connects to the Binance WebSocket Combined Streams (`btcusdt@miniTicker`, `ethusdt@miniTicker`, etc.) to receive real-time price updates. It then publishes these price ticks to the Kafka topic `market-prices`. `Portfolio Service` consumes these updates to keep an in-memory cache up to date, eliminating HTTP overhead during trade executions.
2. **Audit Logging**: `Portfolio Service` publishes transaction records (`ORDER_CREATED`, `ORDER_EXECUTED`, `ORDER_REJECTED`) asynchronously to the `audit-events` Kafka topic. `Audit Service` consumes these events and records them in its database.
3. **HTTP Fallback**: If Kafka is not running, the services automatically fall back to direct HTTP communication to ensure the application remains operational.

### Supported Crypto Coins
The system supports the following real-time assets:
- **BTC** (Bitcoin)
- **ETH** (Ethereum)
- **SOL** (Solana)
- **ADA** (Cardano)
- **XRP** (Ripple)

---

## Database Architecture

The system uses **PostgreSQL** for data persistence. To maintain isolation between microservices, a single PostgreSQL container hosts multiple distinct databases:

1. **`audit_db`** (Audit Service)
   - Stores the `audit_events` table (immutable ledger of all system events).
2. **`portfolio_db`** (Portfolio Service)
   - Stores the `portfolios`, `portfolio_assets`, and `orders` tables.

**Connecting to the Database:**
The PostgreSQL container maps to port **5433** 

| Setting | Value |
|---------|-------|
| Host | `localhost` |
| Port | `5433` |
| User | `postgres` |
| Password | `password` |
| Databases | `audit_db`, `portfolio_db` |


---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust (2021 edition) |
| Database | PostgreSQL (via sqlx) |
| HTTP Framework | actix-web 4 |
| HTTP Client | reqwest 0.11 (with rustls-tls) |
| WebSocket Client | tokio-tungstenite 0.21 (with rustls-tls) |
| Messaging Broker | Apache Kafka (KRaft mode) |
| Kafka Client | rdkafka 0.36.2 (compiled statically) |
| Serialization | serde + serde_json |
| Async Runtime | tokio |
| Logging | tracing + tracing-subscriber |
| Containers | Docker + Docker Compose |

---

## Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (v20+ with Docker Compose V2)
- [Rust](https://www.rust-lang.org/tools/install) (1.88+ for local running)

### Run with Docker Compose (Recommended)

1. Build and boot all containers (Kafka, Postgres, Market, Portfolio, Audit):
   ```bash
   docker compose up -d --build
   ```

2. Monitor container statuses and healthchecks:
   ```bash
   docker compose ps
   ```

3. Run integration tests once all services are healthy:
   ```bash
   chmod +x tests/integration_tests.sh
   ./tests/integration_tests.sh
   ```

4. Stop the services (and remove volumes to reset database):
   ```bash
   docker compose down -v
   ```

---

## API Reference

### 1. Market Service (Port 8081)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/symbols` | List supported crypto coins |
| GET | `/prices` | Get current prices of all coins |
| GET | `/prices/{symbol}` | Get price for a specific coin |

**Example:**
```bash
# Get all symbols
curl http://localhost:8081/symbols

# Get BTC price
curl http://localhost:8081/prices/BTC
```

### 2. Portfolio Service (Port 8082)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/portfolio/{userId}` | Get user portfolio cash & asset balances |
| POST | `/orders` | Submit a market order (BUY/SELL) |
| GET | `/orders/{orderId}` | Get order details |

**Example:**
```bash
# Get portfolio of user1 (Pre-seeded with $100,000 cash)
curl http://localhost:8082/portfolio/user1

# Submit a BUY order for 0.5 BTC
curl -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "BTC", "side": "BUY", "quantity": 0.5}'

# Submit a SELL order for 3 ETH (if owned)
curl -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "ETH", "side": "SELL", "quantity": 3}'
```

### 3. Audit Service (Port 8083)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/events` | List all events (supports filter `?user_id=` or `?event_type=`) |
| GET | `/events/{orderId}` | Get events related to specific order |

**Example:**
```bash
# Get all audit events
curl http://localhost:8083/events
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | None | Address of the Kafka broker (e.g., `kafka:29092`). If empty, runs in REST-only fallback mode. |
| `AUDIT_DATABASE_URL` | None | PostgreSQL connection string for Audit Service. Fallback: `DATABASE_URL`. |
| `PORTFOLIO_DATABASE_URL` | None | PostgreSQL connection string for Portfolio Service. Fallback: `DATABASE_URL`. |
| `DATABASE_URL` | None | Legacy fallback PostgreSQL connection string. |
| `MARKET_SERVICE_URL` | `http://localhost:8081` | REST URL of Market Service (used as fallback by Portfolio Service) |
| `AUDIT_SERVICE_URL` | `http://localhost:8083` | REST URL of Audit Service (used as fallback by Portfolio Service) |
| `HOST` | `0.0.0.0` | Bind address |
| `RUST_LOG` | `info` | Logging filter level |
