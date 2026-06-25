use std::collections::HashMap;
use std::sync::RwLock;

use crate::models::{Order, Portfolio};

/// In-memory store for portfolios and orders.
///
/// Uses `RwLock` for thread-safe concurrent reads and exclusive writes.
/// Pre-seeded with two default users for testing purposes.
pub struct Store {
    pub portfolios: RwLock<HashMap<String, Portfolio>>,
    pub orders: RwLock<HashMap<String, Order>>,
}

impl Store {
    /// Creates a new Store with pre-seeded user portfolios.
    ///
    /// - `user1`: $100,000 cash balance
    /// - `user2`: $50,000 cash balance
    pub fn new() -> Self {
        let mut portfolios = HashMap::new();

        portfolios.insert(
            "user1".to_string(),
            Portfolio {
                user_id: "user1".to_string(),
                cash_balance: 100_000.0,
                assets: HashMap::new(),
            },
        );

        portfolios.insert(
            "user2".to_string(),
            Portfolio {
                user_id: "user2".to_string(),
                cash_balance: 50_000.0,
                assets: HashMap::new(),
            },
        );

        Self {
            portfolios: RwLock::new(portfolios),
            orders: RwLock::new(HashMap::new()),
        }
    }

    /// Gets a clone of a user's portfolio. Returns `None` if user doesn't exist.
    pub fn get_portfolio(&self, user_id: &str) -> Option<Portfolio> {
        let portfolios = self.portfolios.read().unwrap();
        portfolios.get(user_id).cloned()
    }

    /// Stores an order in the order book.
    pub fn save_order(&self, order: Order) {
        let mut orders = self.orders.write().unwrap();
        orders.insert(order.id.clone(), order);
    }

    /// Retrieves an order by its ID.
    pub fn get_order(&self, order_id: &str) -> Option<Order> {
        let orders = self.orders.read().unwrap();
        orders.get(order_id).cloned()
    }

    /// Deducts cash and adds an asset to a user's portfolio (BUY).
    ///
    /// Returns `Err` if the user doesn't exist or has insufficient balance.
    pub fn execute_buy(
        &self,
        user_id: &str,
        symbol: &str,
        quantity: f64,
        total_cost: f64,
    ) -> Result<(), String> {
        let mut portfolios = self.portfolios.write().unwrap();
        let portfolio = portfolios
            .get_mut(user_id)
            .ok_or_else(|| format!("User '{}' not found", user_id))?;

        if portfolio.cash_balance < total_cost {
            return Err(format!(
                "Insufficient balance: have ${:.2}, need ${:.2}",
                portfolio.cash_balance, total_cost
            ));
        }

        portfolio.cash_balance -= total_cost;
        let current_qty = portfolio.assets.get(symbol).copied().unwrap_or(0.0);
        portfolio.assets.insert(symbol.to_string(), current_qty + quantity);

        Ok(())
    }

    /// Deducts an asset and adds cash to a user's portfolio (SELL).
    ///
    /// Returns `Err` if the user doesn't exist or has insufficient assets.
    pub fn execute_sell(
        &self,
        user_id: &str,
        symbol: &str,
        quantity: f64,
        total_value: f64,
    ) -> Result<(), String> {
        let mut portfolios = self.portfolios.write().unwrap();
        let portfolio = portfolios
            .get_mut(user_id)
            .ok_or_else(|| format!("User '{}' not found", user_id))?;

        let current_qty = portfolio.assets.get(symbol).copied().unwrap_or(0.0);
        if current_qty < quantity {
            return Err(format!(
                "Insufficient assets: have {:.4} {}, need {:.4}",
                current_qty, symbol, quantity
            ));
        }

        let new_qty = current_qty - quantity;
        if new_qty < 0.0001 {
            // Remove asset if quantity is essentially zero
            portfolio.assets.remove(symbol);
        } else {
            portfolio.assets.insert(symbol.to_string(), new_qty);
        }
        portfolio.cash_balance += total_value;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_store_has_default_users() {
        let store = Store::new();
        let user1 = store.get_portfolio("user1").unwrap();
        assert_eq!(user1.cash_balance, 100_000.0);
        assert!(user1.assets.is_empty());

        let user2 = store.get_portfolio("user2").unwrap();
        assert_eq!(user2.cash_balance, 50_000.0);
    }

    #[test]
    fn test_execute_buy_success() {
        let store = Store::new();
        let result = store.execute_buy("user1", "AAPL", 10.0, 1785.0);
        assert!(result.is_ok());

        let portfolio = store.get_portfolio("user1").unwrap();
        assert_eq!(portfolio.cash_balance, 100_000.0 - 1785.0);
        assert_eq!(*portfolio.assets.get("AAPL").unwrap(), 10.0);
    }

    #[test]
    fn test_execute_buy_insufficient_balance() {
        let store = Store::new();
        let result = store.execute_buy("user1", "AAPL", 10000.0, 1_785_000.0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Insufficient balance"));
    }

    #[test]
    fn test_execute_sell_success() {
        let store = Store::new();
        // First buy some shares
        store.execute_buy("user1", "AAPL", 10.0, 1785.0).unwrap();
        // Then sell
        let result = store.execute_sell("user1", "AAPL", 5.0, 900.0);
        assert!(result.is_ok());

        let portfolio = store.get_portfolio("user1").unwrap();
        assert_eq!(*portfolio.assets.get("AAPL").unwrap(), 5.0);
        assert_eq!(portfolio.cash_balance, 100_000.0 - 1785.0 + 900.0);
    }

    #[test]
    fn test_execute_sell_insufficient_assets() {
        let store = Store::new();
        let result = store.execute_sell("user1", "AAPL", 5.0, 900.0);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Insufficient assets"));
    }

    #[test]
    fn test_user_not_found() {
        let store = Store::new();
        let result = store.execute_buy("unknown_user", "AAPL", 1.0, 178.5);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }
}
