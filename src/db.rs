//! Запись свечей в PostgreSQL.
//!
//! Фоновый «батчер» получает свечи из mpsc-канала и пишет их в таблицу
//! `candles` пачками в одной транзакции. Upsert идемпотентен: если данные
//! не изменились, обновления строки не происходит (никакого блота).

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc;

use crate::candle::CandleUpdate;

/// Размер пачки перед записью (или таймаут накопления).
const BATCH_SIZE: usize = 500;
const BATCH_FLUSH: Duration = Duration::from_millis(200);

/// Канал, по которому свечи уходят на запись.
pub type CandleSender = mpsc::Sender<CandleUpdate>;

/// Идемпотентный upsert одной свечи.
const UPSERT_ONE: &str = "INSERT INTO candles \
     (exchange, symbol, timeframe, start_ts, open, high, low, close, volume, turnover) \
     VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric, $7::numeric, $8::numeric, $9::numeric, $10::numeric) \
     ON CONFLICT (exchange, symbol, timeframe, start_ts) DO UPDATE SET \
     open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, \
     close = EXCLUDED.close, volume = EXCLUDED.volume, \
     turnover = EXCLUDED.turnover, update_time = now() \
     WHERE (candles.open, candles.high, candles.low, candles.close, candles.volume, candles.turnover) \
         IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume, EXCLUDED.turnover)";

/// Подключается к БД и запускает фоновый писатель; возвращает sender.
pub async fn spawn(db_url: &str) -> Result<CandleSender> {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(Duration::from_secs(10))
        .connect(db_url)
        .await
        .with_context(|| format!("подключение к БД: {db_url}"))?;

    let (tx, mut rx) = mpsc::channel::<CandleUpdate>(BATCH_SIZE * 4);
    tokio::spawn(async move {
        let mut written: u64 = 0;
        let mut errors: u64 = 0;
        loop {
            // Ждём первую свечу пачки.
            let Some(first) = rx.recv().await else {
                break;
            };
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            batch.push(first);
            // Добираем остаток пачки без долгого ожидания.
            let flush_at = tokio::time::Instant::now() + BATCH_FLUSH;
            while batch.len() < BATCH_SIZE {
                tokio::select! {
                    _ = tokio::time::sleep_until(flush_at) => break,
                    c = rx.recv() => match c {
                        Some(c) => batch.push(c),
                        None => break,
                    }
                }
            }
            match upsert_batch(&pool, &batch).await {
                Ok(n) => written += n as u64,
                Err(e) => {
                    errors += 1;
                    eprintln!("[db] ошибка записи пачки из {}: {e:#}", batch.len());
                }
            }
        }
        eprintln!("[db] записано {written} свечей, ошибок {errors}");
    });
    Ok(tx)
}

/// Пишет пачку в одной транзакции.
async fn upsert_batch(pool: &sqlx::PgPool, batch: &[CandleUpdate]) -> Result<usize> {
    if batch.is_empty() {
        return Ok(0);
    }
    let mut tx = pool.begin().await.context("BEGIN")?;
    for c in batch {
        sqlx::query(UPSERT_ONE)
            // ВАЖНО: порядок bind() обязан совпадать с порядком колонок в
            // UPSERT_ONE (см. тест bind_order_matches_columns ниже).
            .bind(&c.exchange)
            .bind(&c.symbol)
            .bind(&c.interval)
            .bind(c.start_ts)
            .bind(c.open)
            .bind(c.high)
            .bind(c.low)
            .bind(c.close)
            .bind(c.volume)
            .bind(c.turnover)
            .execute(&mut *tx)
            .await
            .context("upsert свечи")?;
    }
    tx.commit().await.context("COMMIT")?;
    Ok(batch.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Порядок колонок в UPSERT_ONE. Обязан совпадать с порядком bind()
    /// в upsert_batch — иначе значения уедут в чужие колонки (такая ошибка
    /// однажды уже приводила к high/low/close в неправильных полях).
    const EXPECTED_COLUMNS: [&str; 10] = [
        "exchange",
        "symbol",
        "timeframe",
        "start_ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
    ];

    #[test]
    fn bind_order_matches_columns() {
        let cols_start = UPSERT_ONE.find('(').expect("скобка колонок");
        let cols_end = UPSERT_ONE.find(')').expect("конец списка колонок");
        let columns: Vec<&str> = UPSERT_ONE[cols_start + 1..cols_end]
            .split(',')
            .map(|c| c.trim())
            .collect();
        assert_eq!(columns, EXPECTED_COLUMNS);
    }
}
