use std::collections::HashMap;
use std::sync::RwLock;

/// Price engine that stores real-time market prices.
///
/// Base prices are set as defaults. The values are updated dynamically
/// by a background task fetching from the Binance API.
pub struct PriceEngine {
    base_prices: HashMap<String, f64>,
    current_prices: RwLock<HashMap<String, f64>>,
}

impl PriceEngine {
    /// Creates a new PriceEngine with pre-configured crypto symbols and default prices.
    pub fn new() -> Self {
        let mut base_prices = HashMap::new();
        base_prices.insert("BTC".to_string(), 60000.00);
        base_prices.insert("ETH".to_string(), 3300.00);
        base_prices.insert("SOL".to_string(), 150.00);
        base_prices.insert("ADA".to_string(), 0.45);
        base_prices.insert("XRP".to_string(), 0.50);

        let current_prices = RwLock::new(base_prices.clone());

        Self {
            base_prices,
            current_prices,
        }
    }

    /// Returns a list of all available (symbol, name) pairs.
    pub fn get_symbols(&self) -> Vec<(String, String)> {
        vec![
            ("BTC".to_string(), "Bitcoin".to_string()),
            ("ETH".to_string(), "Ethereum".to_string()),
            ("SOL".to_string(), "Solana".to_string()),
            ("ADA".to_string(), "Cardano".to_string()),
            ("XRP".to_string(), "Ripple".to_string()),
        ]
    }

    /// Gets the current price for a symbol.
    /// Returns `None` if the symbol doesn't exist.
    pub fn get_price(&self, symbol: &str) -> Option<f64> {
        let prices = self.current_prices.read().ok()?;
        prices.get(symbol).copied()
    }

    /// Gets all current prices.
    pub fn get_all_prices(&self) -> HashMap<String, f64> {
        self.current_prices.read().unwrap().clone()
    }

    /// Updates the current price for a symbol.
    pub fn update_price(&self, symbol: &str, price: f64) {
        if let Ok(mut prices) = self.current_prices.write() {
            if prices.contains_key(symbol) {
                prices.insert(symbol.to_string(), price);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_symbols() {
        let engine = PriceEngine::new();
        let symbols = engine.get_symbols();
        assert_eq!(symbols.len(), 5);
    }

    #[test]
    fn test_get_price_valid_symbol() {
        let engine = PriceEngine::new();
        let price = engine.get_price("BTC");
        assert!(price.is_some());
        assert_eq!(price.unwrap(), 60000.00);
    }

    #[test]
    fn test_get_price_invalid_symbol() {
        let engine = PriceEngine::new();
        let price = engine.get_price("INVALID");
        assert!(price.is_none());
    }

    #[test]
    fn test_get_all_prices() {
        let engine = PriceEngine::new();
        let prices = engine.get_all_prices();
        assert_eq!(prices.len(), 5);
        assert!(prices.contains_key("BTC"));
        assert!(prices.contains_key("ETH"));
    }

    #[test]
    fn test_update_price() {
        let engine = PriceEngine::new();
        engine.update_price("BTC", 65000.00);
        assert_eq!(engine.get_price("BTC").unwrap(), 65000.00);
    }
}
