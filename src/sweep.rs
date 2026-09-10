//! Свип закрытых свечей через REST: по каждой паре × интервалу забираем
//! последние закрытые бары и пишем их в БД (или печатаем, если БД не задана).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::Semaphore;

use crate::candle::{candle_to_json_line, from_rest_candle};
use crate::config::Config;
use crate::db::CandleSender;
use crate::kucoin::{RestCandle, fetch_kline_page, forming_bucket_start};

/// Максимум баров, которые REST отдаёт за один запрос (страница).
const PAGE_MAX: usize = 100;
/// Ограничение «глубины» свипа на пару×интервал (страховка).
const BARS_CAP: usize = 1500;

/// Итоги свипа.
#[derive(Debug, Default)]
pub struct Summary {
    pub pairs_ok: usize,
    pub pairs_err: usize,
    pub candles: u64,
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
    api_base: &str,
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
        let page = fetch_with_retries(api_base, symbol, interval, start_at, end_at).await?;
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

/// Запрос истории с парой повторных попыток: сеть/TLS иногда отваливается
/// (`tls handshake eof`), и одна неудачная пара не должна портить свип.
async fn fetch_with_retries(
    api_base: &str,
    symbol: &str,
    interval: &str,
    start_at: Option<i64>,
    end_at: Option<i64>,
) -> Result<Vec<RestCandle>> {
    const ATTEMPTS: u32 = 3;
    let mut last_err = None;
    for attempt in 1..=ATTEMPTS {
        match fetch_kline_page(api_base, symbol, interval, start_at, end_at).await {
            Ok(rows) => return Ok(rows),
            Err(e) => {
                last_err = Some(e);
                if attempt < ATTEMPTS {
                    let pause = Duration::from_millis(300 * u64::from(attempt));
                    tokio::time::sleep(pause).await;
                }
            }
        }
    }
    Err(last_err.expect("ошибка попытки"))
}

/// Один свип по всем парам × интервалам с ограниченной конкурентностью.
pub async fn run(
    cfg: &Config,
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
            let api_base = cfg.api_base.clone();
            let exchange = cfg.exchange.clone();
            let symbol = symbol.clone();
            let interval = interval.clone();
            let sem = sem.clone();
            let db_tx = db_tx.cloned();
            tasks.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore");
                let closed = fetch_closed(&api_base, &symbol, &interval, bars).await?;
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
