#!/bin/bash
##############################################################################
# Integration Tests for Mini Exchange Portfolio System
#
# Prerequisites: All 3 services must be running
#   - Market Service  on port 8081
#   - Portfolio Service on port 8082
#   - Audit Service   on port 8083
#
# Usage:
#   ./tests/integration_tests.sh
#   MARKET_URL=http://localhost:8081 PORTFOLIO_URL=http://localhost:8082 AUDIT_URL=http://localhost:8083 ./tests/integration_tests.sh
##############################################################################

set -e

MARKET_URL="${MARKET_URL:-http://localhost:8081}"
PORTFOLIO_URL="${PORTFOLIO_URL:-http://localhost:8082}"
AUDIT_URL="${AUDIT_URL:-http://localhost:8083}"

PASSED=0
FAILED=0
TOTAL=0

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}  Mini Exchange Portfolio System - Integration Tests${NC}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
}

assert_status() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    TOTAL=$((TOTAL + 1))
    if [ "$actual" -eq "$expected" ]; then
        echo -e "  ${GREEN}✓ PASS${NC} - $test_name (HTTP $actual)"
        PASSED=$((PASSED + 1))
    else
        echo -e "  ${RED}✗ FAIL${NC} - $test_name (expected $expected, got $actual)"
        FAILED=$((FAILED + 1))
    fi
}

assert_contains() {
    local test_name="$1"
    local expected="$2"
    local actual="$3"
    TOTAL=$((TOTAL + 1))
    if echo "$actual" | grep -q "$expected"; then
        echo -e "  ${GREEN}✓ PASS${NC} - $test_name"
        PASSED=$((PASSED + 1))
    else
        echo -e "  ${RED}✗ FAIL${NC} - $test_name (expected to contain '$expected')"
        echo -e "    Response: $actual"
        FAILED=$((FAILED + 1))
    fi
}

##############################################################################
# Health Checks
##############################################################################
echo -e "${YELLOW}▸ Health Checks${NC}"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$MARKET_URL/health")
assert_status "Market Service health check" 200 "$STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PORTFOLIO_URL/health")
assert_status "Portfolio Service health check" 200 "$STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$AUDIT_URL/health")
assert_status "Audit Service health check" 200 "$STATUS"

##############################################################################
# Market Service Tests
##############################################################################
echo ""
echo -e "${YELLOW}▸ Market Service Tests${NC}"

# Test: Get symbols
RESPONSE=$(curl -s "$MARKET_URL/symbols")
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$MARKET_URL/symbols")
assert_status "GET /symbols returns 200" 200 "$STATUS"
assert_contains "Symbols contain BTC" "BTC" "$RESPONSE"
assert_contains "Symbols contain ETH" "ETH" "$RESPONSE"

# Test: Get all prices
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$MARKET_URL/prices")
assert_status "GET /prices returns 200" 200 "$STATUS"

# Test: Get specific price
RESPONSE=$(curl -s "$MARKET_URL/prices/BTC")
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$MARKET_URL/prices/BTC")
assert_status "GET /prices/BTC returns 200" 200 "$STATUS"
assert_contains "Price response has symbol" "BTC" "$RESPONSE"
assert_contains "Price response has price field" "price" "$RESPONSE"

# Test: Invalid symbol returns 404
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$MARKET_URL/prices/INVALID")
assert_status "GET /prices/INVALID returns 404" 404 "$STATUS"

##############################################################################
# Portfolio Service Tests
##############################################################################
echo ""
echo -e "${YELLOW}▸ Portfolio Service Tests${NC}"

# Test: Get initial portfolio
RESPONSE=$(curl -s "$PORTFOLIO_URL/portfolio/user1")
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PORTFOLIO_URL/portfolio/user1")
assert_status "GET /portfolio/user1 returns 200" 200 "$STATUS"
assert_contains "Portfolio has user_id" "user1" "$RESPONSE"
assert_contains "Portfolio has cash_balance" "cash_balance" "$RESPONSE"

# Test: Unknown user returns 404
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PORTFOLIO_URL/portfolio/unknown_user")
assert_status "GET /portfolio/unknown_user returns 404" 404 "$STATUS"

##############################################################################
# Order Tests - Successful BUY
##############################################################################
echo ""
echo -e "${YELLOW}▸ Order Tests - Successful BUY${NC}"

BUY_FULL=$(curl -s -w "\n%{http_code}" -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "BTC", "side": "BUY", "quantity": 0.5}')
BUY_STATUS=$(echo "$BUY_FULL" | tail -1)
BUY_RESPONSE=$(echo "$BUY_FULL" | sed '$d')
assert_status "POST /orders BUY returns 201" 201 "$BUY_STATUS"
assert_contains "BUY order is EXECUTED" "EXECUTED" "$BUY_RESPONSE"

# Extract order ID for later
ORDER_ID=$(echo "$BUY_RESPONSE" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)

# Verify portfolio updated
PORTFOLIO=$(curl -s "$PORTFOLIO_URL/portfolio/user1")
assert_contains "Portfolio has BTC after BUY" "BTC" "$PORTFOLIO"

# Test: Get order by ID
if [ -n "$ORDER_ID" ]; then
    ORDER_RESPONSE=$(curl -s "$PORTFOLIO_URL/orders/$ORDER_ID")
    ORDER_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PORTFOLIO_URL/orders/$ORDER_ID")
    assert_status "GET /orders/{orderId} returns 200" 200 "$ORDER_STATUS"
    assert_contains "Order has correct symbol" "BTC" "$ORDER_RESPONSE"
fi

##############################################################################
# Order Tests - Successful SELL
##############################################################################
echo ""
echo -e "${YELLOW}▸ Order Tests - Successful SELL${NC}"

# First, buy some ETH
curl -s -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "ETH", "side": "BUY", "quantity": 5}' > /dev/null

# Now sell it (use single curl to avoid double request)
SELL_FULL=$(curl -s -w "\n%{http_code}" -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "ETH", "side": "SELL", "quantity": 3}')
SELL_STATUS=$(echo "$SELL_FULL" | tail -1)
SELL_RESPONSE=$(echo "$SELL_FULL" | sed '$d')
assert_status "POST /orders SELL returns 201" 201 "$SELL_STATUS"
assert_contains "SELL order is EXECUTED" "EXECUTED" "$SELL_RESPONSE"

##############################################################################
# Order Tests - Insufficient Balance
##############################################################################
echo ""
echo -e "${YELLOW}▸ Order Tests - Insufficient Balance${NC}"

# user2 has $50,000 - try to buy way more than they can afford (10 BTC = $600k+)
REJECT_RESPONSE=$(curl -s -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user2", "symbol": "BTC", "side": "BUY", "quantity": 10}')
REJECT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user2", "symbol": "BTC", "side": "BUY", "quantity": 10}')
assert_status "BUY with insufficient balance returns 400" 400 "$REJECT_STATUS"
assert_contains "Rejection mentions insufficient balance" "Insufficient balance" "$REJECT_RESPONSE"

##############################################################################
# Order Tests - Insufficient Assets (SELL without owning)
##############################################################################
echo ""
echo -e "${YELLOW}▸ Order Tests - Insufficient Assets${NC}"

SELL_REJECT_RESPONSE=$(curl -s -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user2", "symbol": "SOL", "side": "SELL", "quantity": 100}')
SELL_REJECT_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user2", "symbol": "SOL", "side": "SELL", "quantity": 100}')
assert_status "SELL without assets returns 400" 400 "$SELL_REJECT_STATUS"
assert_contains "Rejection mentions insufficient assets" "Insufficient assets" "$SELL_REJECT_RESPONSE"

##############################################################################
# Order Tests - Invalid Quantity
##############################################################################
echo ""
echo -e "${YELLOW}▸ Order Tests - Invalid Quantity${NC}"

INVALID_QTY_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$PORTFOLIO_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user1", "symbol": "BTC", "side": "BUY", "quantity": -5}')
assert_status "BUY with negative quantity returns 400" 400 "$INVALID_QTY_STATUS"

##############################################################################
# Order Tests - Non-existent Order
##############################################################################
echo ""
echo -e "${YELLOW}▸ Order Tests - Non-existent Order${NC}"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$PORTFOLIO_URL/orders/non-existent-id")
assert_status "GET /orders/non-existent-id returns 404" 404 "$STATUS"

##############################################################################
# Audit Service Tests
##############################################################################
echo ""
echo -e "${YELLOW}▸ Audit Service Tests${NC}"

# Give a moment for async audit events to be sent
sleep 1

EVENTS=$(curl -s "$AUDIT_URL/events")
EVENTS_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$AUDIT_URL/events")
assert_status "GET /events returns 200" 200 "$EVENTS_STATUS"
assert_contains "Events contain ORDER_CREATED" "ORDER_CREATED" "$EVENTS"
assert_contains "Events contain ORDER_EXECUTED" "ORDER_EXECUTED" "$EVENTS"
assert_contains "Events contain ORDER_REJECTED" "ORDER_REJECTED" "$EVENTS"

# Test: Filter by user_id
USER_EVENTS=$(curl -s "$AUDIT_URL/events?user_id=user1")
assert_contains "Filtered events contain user1" "user1" "$USER_EVENTS"

##############################################################################
# Portfolio Correctness After Trades
##############################################################################
echo ""
echo -e "${YELLOW}▸ Portfolio Correctness After Trades${NC}"

FINAL_PORTFOLIO=$(curl -s "$PORTFOLIO_URL/portfolio/user1")
assert_contains "Final portfolio has BTC holdings" "BTC" "$FINAL_PORTFOLIO"
assert_contains "Final portfolio has cash_balance" "cash_balance" "$FINAL_PORTFOLIO"

# user2 should still have close to original balance (orders were rejected)
USER2_PORTFOLIO=$(curl -s "$PORTFOLIO_URL/portfolio/user2")
assert_contains "User2 portfolio exists" "user2" "$USER2_PORTFOLIO"

##############################################################################
# Results Summary
##############################################################################
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  Test Results: $PASSED/$TOTAL passed, $FAILED failed${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if [ "$FAILED" -gt 0 ]; then
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed! ✓${NC}"
    exit 0
fi
