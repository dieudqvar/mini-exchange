use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use tracing::{info, warn};

use crate::models::{Order, Portfolio};

/// Helper struct for reading portfolio_assets rows from PostgreSQL.
#[derive(sqlx::FromRow)]
struct AssetRow {
    symbol: String,
    quantity: f64,
}

/// Helper struct for reading portfolio rows from PostgreSQL.
#[derive(sqlx::FromRow)]
struct PortfolioRow {
    user_id: String,
    cash_balance: f64,
}

/// HFT Store: Operates primarily in-memory for speed, and flushes to PostgreSQL periodically.
pub struct Store {
    portfolios: RwLock<HashMap<String, Portfolio>>,
    orders: RwLock<HashMap<String, Order>>,
    // Tracking dirty state for background flush
    dirty_portfolios: RwLock<HashSet<String>>,
    dirty_orders: RwLock<Vec<Order>>,
    pool: Option<sqlx::PgPool>,
}

impl Store {
    /// Creates a new in-memory Store with pre-seeded user portfolios.
    pub fn new_in_memory() -> Self {
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
            dirty_portfolios: RwLock::new(HashSet::new()),
            dirty_orders: RwLock::new(Vec::new()),
            pool: None,
        }
    }

    /// Creates a new Hybrid Store. Loads state from PostgreSQL into RAM.
    pub async fn new_postgres(pool: sqlx::PgPool) -> Self {
        // 1. Create tables
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS portfolios (
                user_id VARCHAR(255) PRIMARY KEY,
                cash_balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
            );"
        ).execute(&pool).await.expect("Failed to create portfolios table");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS portfolio_assets (
                user_id VARCHAR(255) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                quantity DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                PRIMARY KEY (user_id, symbol)
            );"
        ).execute(&pool).await.expect("Failed to create portfolio_assets table");

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS orders (
                id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                quantity DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION,
                total DOUBLE PRECISION,
                status VARCHAR(20) NOT NULL,
                reject_reason TEXT,
                created_at VARCHAR(255) NOT NULL
            );"
        ).execute(&pool).await.expect("Failed to create orders table");

        // Seed default users if not present
        sqlx::query("INSERT INTO portfolios (user_id, cash_balance) VALUES ('user1', 100000.0) ON CONFLICT (user_id) DO NOTHING;")
            .execute(&pool).await.expect("Failed to seed user1");
        sqlx::query("INSERT INTO portfolios (user_id, cash_balance) VALUES ('user2', 50000.0) ON CONFLICT (user_id) DO NOTHING;")
            .execute(&pool).await.expect("Failed to seed user2");

        // 2. Load data from DB to RAM
        info!("Loading portfolios from database to memory...");
        let rows = sqlx::query_as::<_, PortfolioRow>("SELECT user_id, cash_balance FROM portfolios")
            .fetch_all(&pool)
            .await
            .expect("Failed to load portfolios");

        let mut portfolios_map = HashMap::new();
        for r in rows {
            portfolios_map.insert(
                r.user_id.clone(),
                Portfolio {
                    user_id: r.user_id,
                    cash_balance: r.cash_balance,
                    assets: HashMap::new(),
                },
            );
        }

        #[derive(sqlx::FromRow)]
        struct FullAssetRow {
            user_id: String,
            symbol: String,
            quantity: f64,
        }
        let full_asset_rows = sqlx::query_as::<_, FullAssetRow>("SELECT user_id, symbol, quantity FROM portfolio_assets")
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

        for ar in full_asset_rows {
            if let Some(p) = portfolios_map.get_mut(&ar.user_id) {
                p.assets.insert(ar.symbol, ar.quantity);
            }
        }

        info!("Loaded {} portfolios into memory.", portfolios_map.len());

        Self {
            portfolios: RwLock::new(portfolios_map),
            orders: RwLock::new(HashMap::new()),
            dirty_portfolios: RwLock::new(HashSet::new()),
            dirty_orders: RwLock::new(Vec::new()),
            pool: Some(pool),
        }
    }

    /// Gets a clone of a user's portfolio from memory. Extremely fast.
    pub async fn get_portfolio(&self, user_id: &str) -> Option<Portfolio> {
        let portfolios = self.portfolios.read().unwrap();
        portfolios.get(user_id).cloned()
    }

    /// Retrieves an order by its ID from memory.
    pub async fn get_order(&self, order_id: &str) -> Option<Order> {
        let orders = self.orders.read().unwrap();
        orders.get(order_id).cloned()
    }

    /// Stores an order in memory and marks it for flush.
    pub async fn save_order(&self, order: Order) {
        {
            let mut orders = self.orders.write().unwrap();
            orders.insert(order.id.clone(), order.clone());
        }
        {
            let mut dirty_orders = self.dirty_orders.write().unwrap();
            dirty_orders.push(order);
        }
    }

    fn mark_portfolio_dirty(&self, user_id: &str) {
        let mut dirty = self.dirty_portfolios.write().unwrap();
        dirty.insert(user_id.to_string());
    }

    /// Deducts cash and adds an asset to a user's portfolio (BUY).
    /// Executes entirely in RAM.
    pub async fn execute_buy(
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
        
        drop(portfolios);
        self.mark_portfolio_dirty(user_id);

        Ok(())
    }

    /// Deducts an asset and adds cash to a user's portfolio (SELL).
    /// Executes entirely in RAM.
    pub async fn execute_sell(
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
            portfolio.assets.remove(symbol);
        } else {
            portfolio.assets.insert(symbol.to_string(), new_qty);
        }
        portfolio.cash_balance += total_value;
        
        drop(portfolios);
        self.mark_portfolio_dirty(user_id);

        Ok(())
    }

    /// Periodic background task: Flushes dirty state from RAM to PostgreSQL.
    pub async fn flush_to_db(&self) {
        let pool = match &self.pool {
            Some(p) => p,
            None => return, // In-memory only
        };

        // 1. Extract dirty state and clear the queues (minimize lock time)
        let dirty_users: Vec<String> = {
            let mut dirty = self.dirty_portfolios.write().unwrap();
            let users = dirty.iter().cloned().collect();
            dirty.clear();
            users
        };

        let dirty_orders_list: Vec<Order> = {
            let mut orders = self.dirty_orders.write().unwrap();
            let list = orders.clone();
            orders.clear();
            list
        };

        if dirty_users.is_empty() && dirty_orders_list.is_empty() {
            return; // Nothing to flush
        }

        // Take snapshot of portfolios for writing
        let mut portfolio_snapshots = Vec::new();
        {
            let portfolios = self.portfolios.read().unwrap();
            for uid in &dirty_users {
                if let Some(p) = portfolios.get(uid) {
                    portfolio_snapshots.push(p.clone());
                }
            }
        }

        // 2. Perform Batch DB Writes
        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                warn!("Flush failed to begin transaction: {}", e);
                return;
            }
        };

        for p in portfolio_snapshots {
            let res = sqlx::query("UPDATE portfolios SET cash_balance = $1 WHERE user_id = $2")
                .bind(p.cash_balance)
                .bind(&p.user_id)
                .execute(&mut *tx)
                .await;
            if res.is_err() { continue; }

            let _ = sqlx::query("DELETE FROM portfolio_assets WHERE user_id = $1")
                .bind(&p.user_id)
                .execute(&mut *tx)
                .await;

            for (symbol, qty) in p.assets {
                let _ = sqlx::query("INSERT INTO portfolio_assets (user_id, symbol, quantity) VALUES ($1, $2, $3)")
                    .bind(&p.user_id)
                    .bind(&symbol)
                    .bind(qty)
                    .execute(&mut *tx)
                    .await;
            }
        }

        for o in dirty_orders_list {
            let _ = sqlx::query(
                "INSERT INTO orders (id, user_id, symbol, side, quantity, price, total, status, reject_reason, created_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status, price = EXCLUDED.price,
                    total = EXCLUDED.total, reject_reason = EXCLUDED.reject_reason"
            )
            .bind(&o.id).bind(&o.user_id).bind(&o.symbol).bind(&o.side.to_string())
            .bind(o.quantity).bind(o.price).bind(o.total)
            .bind(&o.status.to_string()).bind(&o.reject_reason).bind(&o.created_at)
            .execute(&mut *tx)
            .await;
        }

        if let Err(e) = tx.commit().await {
            warn!("Failed to commit background DB flush: {}", e);
        } else {
            // Success
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_store_has_default_users() {
        let store = Store::new_in_memory();
        let user1 = store.get_portfolio("user1").await.unwrap();
        assert_eq!(user1.cash_balance, 100_000.0);
        assert!(user1.assets.is_empty());
    }

    #[tokio::test]
    async fn test_execute_buy_success() {
        let store = Store::new_in_memory();
        let result = store.execute_buy("user1", "AAPL", 10.0, 1785.0).await;
        assert!(result.is_ok());

        let portfolio = store.get_portfolio("user1").await.unwrap();
        assert_eq!(portfolio.cash_balance, 100_000.0 - 1785.0);
        assert_eq!(*portfolio.assets.get("AAPL").unwrap(), 10.0);
    }

    #[tokio::test]
    async fn test_execute_buy_insufficient_balance() {
        let store = Store::new_in_memory();
        let result = store.execute_buy("user1", "AAPL", 10000.0, 1_785_000.0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_sell_success() {
        let store = Store::new_in_memory();
        store.execute_buy("user1", "AAPL", 10.0, 1785.0).await.unwrap();
        let result = store.execute_sell("user1", "AAPL", 5.0, 900.0).await;
        assert!(result.is_ok());

        let portfolio = store.get_portfolio("user1").await.unwrap();
        assert_eq!(*portfolio.assets.get("AAPL").unwrap(), 5.0);
        assert_eq!(portfolio.cash_balance, 100_000.0 - 1785.0 + 900.0);
    }
}
