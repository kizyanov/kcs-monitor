//! Свип закрытых свечей через REST: по каждой паре × интервалу забираем
//! последние закрытые бары и пишем их в БД (или печатаем, если БД не задана).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Semaphore;

use crate::candle::{candle_to_json_line, from_rest_candle};
use crate::config::Config;
use crate::db::CandleSender;
use crate::http::{HttpClient, RateLimited, jitter_ms};
use crate::kucoin::{RestCandle, fetch_kline_page, forming_bucket_start};

/// Максимум баров, которые REST отдаёт за один запрос (страница).
const PAGE_MAX: usize = 100;
/// Ограничение «глубины» свипа на пару×интервал (страховка).
const BARS_CAP: usize = 1500;
/// Число попыток на страницу свечей.
const ATTEMPTS: u32 = 5;
/// Потолок выдержки между попытками.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Итоги свипа.
#[derive(Debug, Default)]
pub struct Summary {
    pub pairs_ok: usize,
    pub pairs_err: usize,
    pub candles: u64,
    /// Сколько пар×интервалов так и не удалось забрать из-за 429.
    pub rate_limited: usize,
}

/// Выдержка перед повтором: экспонента от номера попытки плюс джиттер.
///
/// Для 429 база меньше — саму паузу уже держит общий лимитер (по заголовкам
/// `gw-ratelimit-*`), здесь нужно лишь развести задачи во времени.
fn backoff_delay(attempt: u32, rate_limited: bool, jitter: u64) -> Duration {
    let base_ms = if rate_limited { 250 } else { 300 };
    let exp = base_ms << (attempt.saturating_sub(1)).min(4);
    let jitter = jitter % (base_ms + 1);
    Duration::from_millis(exp.min(MAX_BACKOFF.as_millis() as u64) + jitter)
}

/// Печатает свечу в stdout.
fn emit_line(c: &crate::candle::CandleUpdate) {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "{}", candle_to_json_line(c));
}

/// Забирает с REST до `bars` последних ЗАКРЫТЫХ свечей (новые сверху).
///
/// Первый запрос ограничиваем окном `startAt`, чтобы биржа не отдавала
/// лишние строки: при 3 барах это вместо 100 строк пары — 3.
async fn fetch_closed(
    client: &HttpClient,
    symbol: &str,
    interval: &str,
    bars: usize,
) -> Result<Vec<RestCandle>> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let period = crate::kucoin::interval_seconds(interval)
        .ok_or_else(|| anyhow::anyhow!("неизвестный интервал {interval}"))? as i64;
    let forming = forming_bucket_start(interval, now)
        .ok_or_else(|| anyhow::anyhow!("неизвестный интервал {interval}"))?;

    let cap = bars.min(BARS_CAP);
    let mut closed: Vec<RestCandle> = Vec::with_capacity(cap);
    // Первая страница: строго окно нужных баров (чуть шире — на погранслучаи).
    let mut start_at: Option<i64> = Some(forming - period * (cap as i64 + 1));
    let mut end_at: Option<i64> = Some(now);
    let mut pages = 0usize;

    while closed.len() < cap && pages < BARS_CAP / PAGE_MAX + 2 {
        let page = fetch_with_retries(client, symbol, interval, start_at, end_at).await?;
        pages += 1;
        if page.is_empty() {
            break;
        }
        let oldest = page.last().expect("not empty").start_ts;
        // Строки текущего (незакрытого) бара отбрасываем.
        for c in page {
            if c.start_ts < forming {
                closed.push(c);
            }
        }
        if closed.len() >= cap {
            break;
        }
        // Нужно глубже: идём в прошлое страницами (по 100 строк).
        start_at = None;
        let next_end = oldest - 1;
        if Some(next_end) >= end_at {
            break; // защита от зацикливания
        }
        end_at = Some(next_end);
    }

    closed.truncate(cap);
    Ok(closed)
}

/// Запрос истории с повторными попытками: сеть/TLS иногда отваливается
/// (`tls handshake eof`), а биржа изредка отвечает 429 — одна неудачная пара
/// не должна портить свип.
///
/// При 429 общий лимитер уже поставил на паузу все задачи процесса, поэтому
/// здесь добавляем только короткую выдержку с джиттером, чтобы задачи не
/// проснулись синхронно и не повторили залп.
async fn fetch_with_retries(
    client: &HttpClient,
    symbol: &str,
    interval: &str,
    start_at: Option<i64>,
    end_at: Option<i64>,
) -> Result<Vec<RestCandle>> {
    let mut last_err = None;
    for attempt in 1..=ATTEMPTS {
        match fetch_kline_page(client, symbol, interval, start_at, end_at).await {
            Ok(rows) => return Ok(rows),
            Err(e) => {
                let rate_limited = e.downcast_ref::<RateLimited>().is_some();
                last_err = Some(e);
                if attempt < ATTEMPTS {
                    tokio::time::sleep(backoff_delay(attempt, rate_limited, jitter_ms())).await;
                }
            }
        }
    }
    Err(last_err.expect("ошибка попытки"))
}

/// Один свип по всем парам × интервалам с ограниченной конкурентностью.
pub async fn run(
    cfg: &Config,
    client: &Arc<HttpClient>,
    symbols: &[String],
    bars: usize,
    db_tx: Option<&CandleSender>,
) -> Summary {
    let mut summary = Summary::default();
    if symbols.is_empty() || cfg.kline_intervals.is_empty() {
        return summary;
    }
    let sem = Arc::new(Semaphore::new(cfg.concurrency));
    let mut tasks = tokio::task::JoinSet::new();

    for symbol in symbols {
        for interval in &cfg.kline_intervals {
            let client = client.clone();
            let exchange = cfg.exchange.clone();
            let symbol = symbol.clone();
            let interval = interval.clone();
            let sem = sem.clone();
            let db_tx = db_tx.cloned();
            tasks.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                let closed = fetch_closed(&client, &symbol, &interval, bars).await?;
                let mut n = 0u64;
                for c in closed {
                    let update = from_rest_candle(&exchange, &symbol, &interval, c);
                    match &db_tx {
                        Some(tx) => {
                            tx.send(update).await.ok();
                        }
                        None => emit_line(&update),
                    }
                    n += 1;
                }
                Ok::<u64, anyhow::Error>(n)
            });
        }
    }

    while let Some(res) = tasks.join_next().await {
        match res {
            Ok(Ok(n)) => {
                summary.pairs_ok += 1;
                summary.candles += n;
            }
            Ok(Err(e)) => {
                summary.pairs_err += 1;
                if e.downcast_ref::<RateLimited>().is_some() {
                    summary.rate_limited += 1;
                }
                eprintln!("[sweep] ошибка: {e:#}");
            }
            Err(e) => {
                summary.pairs_err += 1;
                eprintln!("[sweep] задача упала: {e}");
            }
        }
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        let first = backoff_delay(1, false, 0);
        let second = backoff_delay(2, false, 0);
        let third = backoff_delay(3, false, 0);
        assert_eq!(first, Duration::from_millis(300));
        assert_eq!(second, Duration::from_millis(600));
        assert_eq!(third, Duration::from_millis(1200));
        // Экспонента не растёт бесконечно и не превышает потолок с джиттером.
        for attempt in 1..=10 {
            for jitter in [0, 250, 999] {
                let d = backoff_delay(attempt, false, jitter);
                assert!(d <= MAX_BACKOFF + Duration::from_millis(300), "{d:?}");
            }
        }
    }

    #[test]
    fn rate_limited_backoff_keeps_jitter_within_base() {
        for jitter in [0, 100, 999] {
            let d = backoff_delay(1, true, jitter);
            assert!(d >= Duration::from_millis(250), "{d:?}");
            assert!(d <= Duration::from_millis(500), "{d:?}");
        }
    }
}
