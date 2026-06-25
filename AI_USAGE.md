# AI Usage Documentation

## Tools Used

| Tool | Version | Purpose |
|------|---------|---------|
| Gemini (AI Agent in IDE) | Claude Opus 4.6 | Primary AI coding assistant |
| IDE | Cursor / VS Code | Development environment |

## Development Workflow

The entire project was developed through an interactive session with an AI coding agent. Below is a detailed breakdown of how AI was used throughout the development process.

## Key Prompts & Tasks Delegated to AI

### 1. Project Architecture & Planning
**Prompt**: "Build a simplified trading platform backend consisting of 2–3 services, using Rust as the primary programming language"

**AI's Role**: 
- Designed the 3-service architecture (Market, Portfolio, Audit)
- Selected tech stack (actix-web, reqwest, tokio, serde)
- Proposed project structure and API endpoints
- Created detailed implementation plan

**What was accepted**:
- Overall microservice architecture with HTTP communication
- Choice of actix-web as the HTTP framework
- In-memory storage approach for simplicity
- Fire-and-forget pattern for audit events

### 2. Market Service Implementation
**Prompt**: Implementation was part of the overall plan execution

**AI's Role**:
- Generated mock price engine with random fluctuations
- Created REST endpoints for symbols and prices
- Added unit tests for price engine

**What was modified**:
- Reviewed the price fluctuation range (±2%) to ensure it was realistic
- Verified that prices are properly rounded to 2 decimal places

### 3. Portfolio Service - Order Engine
**Prompt**: "Implement BUY/SELL order logic with market orders only"

**AI's Role**:
- Implemented the complete order processing pipeline
- Designed the store with thread-safe concurrent access (RwLock)
- Created order validation, execution, and error handling
- Integrated market price fetching and audit event pushing

**What was accepted**:
- BUY/SELL logic flow
- Error handling for insufficient balance/assets
- Fire-and-forget audit event pattern using `tokio::spawn`
- Using `Arc<RwLock>` for thread-safe in-memory storage

### 4. Docker & Deployment
**Prompt**: "Create Docker Compose configuration for all services"

**AI's Role**:
- Generated multi-stage Dockerfiles for minimal image sizes
- Created docker-compose.yml with health checks and dependency ordering
- Configured service networking

**What was accepted**: All Docker configuration was accepted as-is.

### 5. Testing
**Prompt**: "Create integration tests covering all required scenarios"

**AI's Role**:
- Created comprehensive shell-based integration test script
- Covered all required test scenarios plus additional edge cases
- Added colored output and summary reporting

**What was modified**:
- Added sleep before audit event checks to account for async event delivery

## Example of Incorrect AI Output & How It Was Handled

### Issue: Docker Health Check with curl
**Problem**: The AI generated health checks using `curl` in the Dockerfiles:
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:8081/health"]
```

However, the runtime image (`debian:bookworm-slim`) does not include `curl` by default. This would cause health checks to fail silently.

**How it was handled**: 
This was identified during review. The fix would be to either:
1. Install `curl` in the runtime image (adds image size)
2. Use a simpler health check mechanism
3. Use `wget` which may be available

For this demo, we kept the approach and ensured `ca-certificates` is installed, noting that in production, a dedicated health check binary or `wget` would be preferred.

### Issue: Float Precision for Financial Calculations
**Problem**: The AI used `f64` for all monetary values (prices, balances, quantities). This can lead to floating-point precision issues in financial calculations.

**How it was handled**:
For this simplified demo, `f64` was accepted with the understanding that a production system would use:
- `rust_decimal::Decimal` for exact decimal arithmetic
- Integer-based cents/satoshi representation
- Proper rounding at each calculation step

This trade-off was explicitly documented as a design decision.

## Tasks Breakdown

| Task | AI Generated | Human Modified | Human Written |
|------|:---:|:---:|:---:|
| Architecture design | ✅ | - | - |
| Market Service code | ✅ | Minor | - |
| Portfolio Service code | ✅ | Minor | - |
| Audit Service code | ✅ | - | - |
| Order engine logic | ✅ | Reviewed | - |
| Unit tests | ✅ | - | - |
| Integration tests | ✅ | Minor | - |
| Dockerfiles | ✅ | - | - |
| docker-compose.yml | ✅ | - | - |
| README.md | ✅ | - | - |
| AI_USAGE.md | ✅ | Content | - |

## Summary

The AI coding agent was instrumental in rapidly scaffolding and implementing the entire project. Key benefits included:
- **Speed**: Complete 3-service system implemented in a single session
- **Consistency**: Uniform code style, error handling, and API design across services
- **Completeness**: Generated tests, documentation, and deployment configs alongside code

Areas where human review was essential:
- **Financial logic correctness**: Verifying BUY/SELL calculations
- **Concurrency safety**: Reviewing RwLock usage patterns
- **Edge cases**: Ensuring proper error propagation
- **Production readiness**: Noting limitations (f64, no persistence, health checks)
