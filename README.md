# Mini Exchange Portfolio System

A simplified trading platform backend consisting of 3 microservices, built with **Rust**.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌───────────────┐
│   Market     │◄────│   Portfolio       │────►│   Audit       │
│   Service    │     │   Service         │     │   Service     │
│   :8081      │     │   :8082           │     │   :8083       │
│              │     │                   │     │               │
│ • Symbols    │     │ • Portfolio mgmt  │     │ • Event log   │
│ • Prices     │     │ • Order execution │     │ • Filtering   │
│ • Mock data  │     │ • BUY/SELL logic  │     │ • Audit trail │
└─────────────┘     └──────────────────┘     └───────────────┘
```

### Service Communication
- **Portfolio → Market**: HTTP GET to fetch current prices during order execution
- **Portfolio → Audit**: HTTP POST to push audit events (fire-and-forget)

### Data Storage
- All services use **in-memory storage** (data resets on restart)
- Pre-seeded users: `user1` ($100,000), `user2` ($50,000)

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Rust (2021 edition) |
| HTTP Framework | actix-web 4 |
| HTTP Client | reqwest 0.11 |
| Serialization | serde + serde_json |
| Async Runtime | tokio |
| Logging | tracing + tracing-subscriber |
| Containers | Docker + Docker Compose |

## Quick Start

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install) (1.70+)
- [Docker](https://docs.docker.com/get-docker/) & Docker Compose (optional)

### Option 1: Docker Compose (Recommended)

```bash
# Build and start all services
docker compose up --build

# In another terminal, run integration tests
chmod +x tests/integration_tests.sh
./tests/integration_tests.sh
```

### Option 2: Run Locally (without Docker)

```bash
# Terminal 1: Start Market Service
cargo run --bin market-service

# Terminal 2: Start Audit Service
cargo run --bin audit-service

# Terminal 3: Start Portfolio Service
cargo run --bin portfolio-service

# Terminal 4: Run tests
chmod +x tests/integration_tests.sh
./tests/integration_tests.sh
```

### Run Unit Tests

```bash
cargo test --workspace
```

## API Reference

### Market Service (Port 8081)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/symbols` | List all tradeable symbols |
| GET | `/prices` | Get all current prices |
| GET | `/prices/{symbol}` | Get price for a specific symbol |

**Example:**
```bash
# Get all symbols
curl http://localhost:8081/symbols

# Get AAPL price
curl http://localhost:8081/prices/AAPL
```

### Portfolio Service (Port 8082)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/portfolio/{userId}` | Get user portfolio |
| POST | `/orders` | Submit a market order |
| GET | `/orders/{orderId}` | Get order status |

**Example:**
```bash
# Get portfolio
curl http://localhost:8082/portfolio/user1

# Place a BUY order
curl -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "AAPL", "side": "BUY", "quantity": 10}'

# Place a SELL order
curl -X POST http://localhost:8082/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "AAPL", "side": "SELL", "quantity": 5}'
```

### Audit Service (Port 8083)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| POST | `/events` | Receive audit event |
| GET | `/events` | List events (filter: `?user_id=`, `?event_type=`) |
| GET | `/events/{orderId}` | Get events for an order |

**Example:**
```bash
# Get all audit events
curl http://localhost:8083/events

# Filter by user
curl http://localhost:8083/events?user_id=user1
```

## Order Logic

### Market Orders Only
- **BUY**: Fetch price → Check balance ≥ total cost → Deduct cash → Add asset
- **SELL**: Check asset quantity ≥ sell quantity → Deduct asset → Add cash

### Error Handling
- **Insufficient balance**: Returns 400 with error message
- **Insufficient assets**: Returns 400 with error message
- **Market service unavailable**: Returns 503 with error message
- **Invalid symbol**: Propagated from Market Service as 503
- **User not found**: Returns 404

## Testing

### Test Scenarios Covered

| # | Scenario | Type |
|---|----------|------|
| 1 | Successful BUY order | ✅ Happy path |
| 2 | Successful SELL order | ✅ Happy path |
| 3 | Insufficient balance BUY | ❌ Error case |
| 4 | Insufficient assets SELL | ❌ Error case |
| 5 | Invalid quantity (negative) | ❌ Error case |
| 6 | Portfolio updates after trades | ✅ Verification |
| 7 | Audit events captured | ✅ Verification |
| 8 | Non-existent order lookup | ❌ Error case |
| 9 | Unknown user portfolio | ❌ Error case |
| 10 | Invalid symbol price | ❌ Error case |

### Running Tests

```bash
# Unit tests
cargo test --workspace

# Integration tests (services must be running)
./tests/integration_tests.sh
```

## Environment Variables

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `HOST` | All | `0.0.0.0` | Bind address |
| `PORT` | Market | `8081` | Service port |
| `PORT` | Portfolio | `8082` | Service port |
| `PORT` | Audit | `8083` | Service port |
| `MARKET_SERVICE_URL` | Portfolio | `http://localhost:8081` | Market service URL |
| `AUDIT_SERVICE_URL` | Portfolio | `http://localhost:8083` | Audit service URL |
| `RUST_LOG` | All | `info` | Log level |

## Project Structure

```
mini-exchange/
├── Cargo.toml                  # Workspace root
├── docker-compose.yml
├── Dockerfile.market
├── Dockerfile.portfolio
├── Dockerfile.audit
├── README.md
├── AI_USAGE.md
├── market-service/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs             # Server setup & routing
│       ├── handlers.rs         # HTTP request handlers
│       ├── models.rs           # Data structures
│       └── price_engine.rs     # Mock price generation
├── portfolio-service/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs             # Server setup & routing
│       ├── handlers.rs         # HTTP request handlers
│       ├── models.rs           # Data structures
│       ├── order_engine.rs     # Order processing logic
│       └── store.rs            # In-memory data store
├── audit-service/
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs             # Server setup & routing
│       ├── handlers.rs         # HTTP request handlers
│       └── models.rs           # Data structures
└── tests/
    └── integration_tests.sh    # End-to-end test suite
```
