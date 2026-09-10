//! KuCoin REST: список торгуемых символов и история свечей.

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const DEFAULT_API_BASE: &str = "https://api.kucoin.com";
const OK_CODE: &str = "200000";

/// Допустимые типы свечей KuCoin (spot) — используются в параметре `type`.
pub const VALID_KLINE_INTERVALS: &[&str] = &[
    "1min", "3min", "5min", "15min", "30min", "1hour", "2hour", "4hour", "6hour", "8hour",
    "12hour", "1day", "1week",
];

/// Длина интервала в секундах.
pub fn interval_seconds(interval: &str) -> Option<u64> {
    Some(match interval {
        "1min" => 60,
        "3min" => 180,
        "5min" => 300,
        "15min" => 900,
        "30min" => 1800,
        "1hour" => 3600,
        "2hour" => 7200,
        "4hour" => 14_400,
        "6hour" => 21_600,
        "8hour" => 28_800,
        "12hour" => 43_200,
        "1day" => 86_400,
        "1week" => 604_800,
        _ => return None,
    })
}

/// Начало текущего (ещё не закрытого) бара по времени `now` (unix-сек).
/// Всё, что имеет `start_ts` меньше этого значения, — закрытые свечи.
///
/// Все интервалы KuCoin выровнены по эпохе Unix без смещений: проверено по API,
/// недельные свечи начинаются в четверг 00:00 UTC (эпоха тоже стартовала в
/// четверг), поэтому отдельная поправка для `1week` не нужна.
pub fn forming_bucket_start(interval: &str, now: i64) -> Option<i64> {
    let period = interval_seconds(interval)? as i64;
    Some(now / period * period)
}

/// Строка свечи из REST: [start, open, close, high, low, volume, turnover].
#[derive(Debug, Clone)]
pub struct RestCandle {
    pub start_ts: i64,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub turnover: f64,
}

#[derive(Debug, Deserialize)]
struct Symbol {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "enableTrading", default)]
    enable_trading: bool,
}

/// Проверяет код ответа KuCoin API.
fn ensure_ok(v: &serde_json::Value) -> Result<()> {
    match v.get("code").and_then(|c| c.as_str()) {
        Some(OK_CODE) => Ok(()),
        other => bail!(
            "KuCoin API code={:?} msg={:?}",
            other,
            v.get("msg").and_then(|m| m.as_str())
        ),
    }
}

/// Возвращает отсортированный список торгуемых пар (или отфильтрованный по
/// `symbols_override`, если он задан).
pub async fn fetch_symbols(
    api_base: &str,
    symbols_override: &Option<String>,
) -> Result<Vec<String>> {
    let v = crate::http::get_json(api_base, "/api/v1/symbols").await?;
    ensure_ok(&v)?;
    let symbols: Vec<Symbol> = serde_json::from_value(
        v.get("data")
            .cloned()
            .context("нет data в ответе symbols")?,
    )
    .context("не удалось разобрать список символов")?;

    let mut list: Vec<String> = symbols
        .into_iter()
        .filter(|s| s.enable_trading)
        .map(|s| s.symbol)
        .collect();

    if let Some(filter) = symbols_override {
        let wanted: std::collections::HashSet<String> =
            filter.split(',').map(|s| s.trim().to_string()).collect();
        list.retain(|s| wanted.contains(s));
    }
    list.sort();
    list.dedup();
    Ok(list)
}

/// Запрашивает страницу свечей (новые сверху, до ~100 строк) за окно
/// [start_at, end_at] (unix-сек; None = без соответствующей границы).
pub async fn fetch_kline_page(
    api_base: &str,
    symbol: &str,
    interval: &str,
    start_at: Option<i64>,
    end_at: Option<i64>,
) -> Result<Vec<RestCandle>> {
    let mut params = vec![
        ("type", interval.to_string()),
        ("symbol", symbol.to_string()),
    ];
    if let Some(s) = start_at {
        params.push(("startAt", s.to_string()));
    }
    if let Some(e) = end_at {
        params.push(("endAt", e.to_string()));
    }
    let path = format!(
        "/api/v1/market/candles?{}",
        params
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&")
    );
    let v = crate::http::get_json(api_base, &path).await?;
    ensure_ok(&v)?;

    let rows: Vec<Vec<String>> = serde_json::from_value(
        v.get("data")
            .cloned()
            .context("нет data в ответе candles")?,
    )
    .context("не удалось разобрать строки свечей")?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != 7 {
            bail!("свеча не из 7 полей: {row:?}");
        }
        let num = |i: usize| -> Result<f64> {
            row[i].parse().with_context(|| format!("число в {row:?}"))
        };
        out.push(RestCandle {
            start_ts: row[0].parse().with_context(|| format!("start в {row:?}"))?,
            open: num(1)?,
            close: num(2)?,
            high: num(3)?,
            low: num(4)?,
            volume: num(5)?,
            turnover: num(6)?,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_seconds_known_values() {
        assert_eq!(interval_seconds("1min"), Some(60));
        assert_eq!(interval_seconds("1hour"), Some(3600));
        assert_eq!(interval_seconds("1day"), Some(86_400));
        assert_eq!(interval_seconds("1week"), Some(604_800));
        assert_eq!(interval_seconds("2days"), None);
    }

    #[test]
    fn hour_bucket_is_utc_aligned() {
        let now = 1_788_947_696; // 2026-09-09 12:34:56 UTC
        let start = forming_bucket_start("1hour", now).unwrap();
        assert_eq!(start % 3600, 0);
        assert!(start <= now && now - start < 3600);
    }

    #[test]
    fn weekly_bucket_matches_kucoin_anchor() {
        // KuCoin начинает недельные свечи в четверг 00:00 UTC — значения взяты
        // из реального ответа API (BTC-USDT, type=1week).
        let week = 1_788_393_600; // четверг 2026-09-03 00:00 UTC
        for day in 0..7 {
            let now = week + day * 86_400 + 3600;
            assert_eq!(forming_bucket_start("1week", now), Some(week), "day {day}");
        }
        let next_week = week + 7 * 86_400; // четверг 2026-09-10
        assert_eq!(forming_bucket_start("1week", next_week), Some(next_week));
        assert_eq!(
            forming_bucket_start("1week", next_week + 6 * 86_400),
            Some(next_week)
        );
    }
}
