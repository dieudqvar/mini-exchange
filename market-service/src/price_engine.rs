use rand::Rng;
use std::collections::HashMap;
use std::sync::RwLock;

/// Mock price engine that simulates market prices with random fluctuations.
///
/// Base prices are set for well-known symbols. Each call to `get_price`
/// applies a small random fluctuation (±2%) to simulate real market movement.
pub struct PriceEngine {
    base_prices: HashMap<String, f64>,
    current_prices: RwLock<HashMap<String, f64>>,
}

impl PriceEngine {
    /// Creates a new PriceEngine with pre-configured symbols and base prices.
    pub fn new() -> Self {
        let mut base_prices = HashMap::new();
        base_prices.insert("AAPL".to_string(), 178.50);
        base_prices.insert("GOOGL".to_string(), 141.80);
        base_prices.insert("MSFT".to_string(), 378.90);
        base_prices.insert("AMZN".to_string(), 185.60);
        base_prices.insert("TSLA".to_string(), 248.40);
        base_prices.insert("META".to_string(), 505.75);
        base_prices.insert("NVDA".to_string(), 130.50);
        base_prices.insert("NFLX".to_string(), 640.20);

        let current_prices = RwLock::new(base_prices.clone());

        Self {
            base_prices,
            current_prices,
        }
    }

    /// Returns a list of all available (symbol, name) pairs.
    pub fn get_symbols(&self) -> Vec<(String, String)> {
        let symbol_names: Vec<(String, String)> = vec![
            ("AAPL".to_string(), "Apple Inc.".to_string()),
            ("GOOGL".to_string(), "Alphabet Inc.".to_string()),
            ("MSFT".to_string(), "Microsoft Corp.".to_string()),
            ("AMZN".to_string(), "Amazon.com Inc.".to_string()),
            ("TSLA".to_string(), "Tesla Inc.".to_string()),
            ("META".to_string(), "Meta Platforms Inc.".to_string()),
            ("NVDA".to_string(), "NVIDIA Corp.".to_string()),
            ("NFLX".to_string(), "Netflix Inc.".to_string()),
        ];
        symbol_names
    }

    /// Gets the current price for a symbol, applying a random fluctuation.
    /// Returns `None` if the symbol doesn't exist.
    pub fn get_price(&self, symbol: &str) -> Option<f64> {
        let base = self.base_prices.get(symbol)?;
        let mut rng = rand::thread_rng();

        // Apply ±2% fluctuation from base price
        let fluctuation = rng.gen_range(-0.02..=0.02);
        let new_price = base * (1.0 + fluctuation);
        let rounded = (new_price * 100.0).round() / 100.0;

        // Update current price
        if let Ok(mut prices) = self.current_prices.write() {
            prices.insert(symbol.to_string(), rounded);
        }

        Some(rounded)
    }

    /// Gets all current prices with fresh fluctuations applied.
    pub fn get_all_prices(&self) -> HashMap<String, f64> {
        let mut prices = HashMap::new();
        for symbol in self.base_prices.keys() {
            if let Some(price) = self.get_price(symbol) {
                prices.insert(symbol.clone(), price);
            }
        }
        prices
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_symbols() {
        let engine = PriceEngine::new();
        let symbols = engine.get_symbols();
        assert_eq!(symbols.len(), 8);
    }

    #[test]
    fn test_get_price_valid_symbol() {
        let engine = PriceEngine::new();
        let price = engine.get_price("AAPL");
        assert!(price.is_some());
        let price = price.unwrap();
        // Should be within ±2% of base price (178.50)
        assert!(price > 174.0 && price < 183.0);
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
        assert_eq!(prices.len(), 8);
        assert!(prices.contains_key("AAPL"));
        assert!(prices.contains_key("GOOGL"));
    }
}
