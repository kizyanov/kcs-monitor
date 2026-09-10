//! Закрытая свеча и её представление для записи/вывода.

use crate::kucoin::RestCandle;

/// Закрытая свеча (одна строка в таблице `candles`).
#[derive(Debug, Clone)]
pub struct CandleUpdate {
    pub exchange: String,
    pub symbol: String,
    pub interval: String,
    /// Начало свечи, unix-секунды (UTC).
    pub start_ts: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub turnover: f64,
}

/// Собирает свечу из REST-строки.
pub fn from_rest_candle(
    exchange: &str,
    symbol: &str,
    interval: &str,
    c: RestCandle,
) -> CandleUpdate {
    CandleUpdate {
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        interval: interval.to_string(),
        start_ts: c.start_ts,
        open: c.open,
        close: c.close,
        high: c.high,
        low: c.low,
        volume: c.volume,
        turnover: c.turnover,
    }
}

/// JSON-строка свечи (выводится, когда БД не задана).
pub fn candle_to_json_line(c: &CandleUpdate) -> String {
    serde_json::json!({
        "type": "candle",
        "exchange": c.exchange,
        "symbol": c.symbol,
        "interval": c.interval,
        "start": c.start_ts,
        "open": c.open,
        "close": c.close,
        "high": c.high,
        "low": c.low,
        "volume": c.volume,
        "turnover": c.turnover,
    })
    .to_string()
}
