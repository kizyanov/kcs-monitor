//! kcs-monitor: загрузка закрытых свечей KuCoin в PostgreSQL.
//!
//! Цикл работы:
//!   1. REST `/api/v1/symbols` — список торгуемых пар (обновляется каждый цикл,
//!      поэтому новые листинги подхватываются сами);
//!   2. по каждой паре × таймфрейму — последние ЗАКРЫТЫЕ бары
//!      (`/api/v1/market/candles`, текущая незакрытая свеча отбрасывается);
//!   3. идемпотентный upsert в таблицу `candles` (без изменений — без записи);
//!   4. пауза `KCS_INTERVAL_SECONDS` (по умолчанию 600 с) и повтор.
//!      `KCS_INTERVAL_SECONDS=0` — один проход и выход (для cron);
//!   5. без `DATABASE_URL` свечи печатаются JSON-строками в stdout.
//!
//! Остановка: SIGINT/SIGTERM — процесс завершает текущий свип, сбрасывает
//! буфер записи и выходит с кодом 0.
//!
//! Миграции живут отдельно (проект sqlxmigrator), приложение их не трогает.

mod candle;
mod config;
mod db;
mod http;
mod kucoin;
mod sweep;

use std::sync::Arc;
use std::time::Duration;

use config::Config;
use http::{HttpClient, RateLimiter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Читаем .env из текущего каталога (если есть); уже заданные переменные
    // окружения имеют приоритет и не перезаписываются.
    let _ = dotenvy::dotenv();
    let cfg = Config::from_env();

    // Проверяем интервалы, чтобы опечатка не упала молча на запросе.
    for interval in &cfg.kline_intervals {
        if !kucoin::VALID_KLINE_INTERVALS.contains(&interval.as_str()) {
            anyhow::bail!(
                "неизвестный интервал '{interval}'; допустимые: {}",
                kucoin::VALID_KLINE_INTERVALS.join(", ")
            );
        }
    }
    eprintln!(
        "kcs-monitor: exchange={} intervals={} bars={} (первый свип: {}) concurrency={} пауза={} api={}",
        cfg.exchange,
        cfg.kline_intervals.join(","),
        cfg.bars,
        if cfg.bars_first > 0 {
            cfg.bars_first.to_string()
        } else {
            "как обычный".to_string()
        },
        cfg.concurrency,
        if cfg.interval_secs == 0 {
            "нет (один проход)".to_string()
        } else {
            format!("{}s", cfg.interval_secs)
        },
        cfg.api_base
    );

    // Один клиент и один лимитер на процесс: темп запросов считается по весу
    // эндпоинтов (публичный пул KuCoin ограничен по IP), а 429 ставит на паузу
    // сразу все задачи, а не только ту, что его получила.
    let limiter = Arc::new(RateLimiter::new(cfg.rate_limit_weight_per_sec));
    let client = Arc::new(HttpClient::new(&cfg.api_base, limiter)?);
    eprintln!(
        "kcs-monitor: лимит запросов {} weight/s (публичный пул KuCoin: 4000 weight/30s на IP)",
        cfg.rate_limit_weight_per_sec
    );

    // Запись в БД (если задан DATABASE_URL), иначе вывод в stdout.
    // Писатель живёт все циклы и закрывается при выходе.
    let db_tx: Option<db::CandleSender> = match &cfg.db_url {
        Some(url) => {
            eprintln!("kcs-monitor: подключение к БД ...");
            Some(db::spawn(url).await?)
        }
        None => {
            eprintln!("kcs-monitor: БД не задана — вывожу свечи в stdout");
            None
        }
    };

    let mut sweep_no: u64 = 0;
    loop {
        sweep_no += 1;
        let started = std::time::Instant::now();

        // Список пар обновляем каждый цикл.
        let symbols = tokio::select! {
            _ = wait_for_shutdown() => break,
            res = kucoin::fetch_symbols(&client, &cfg.symbols_override) => res?,
        };
        if symbols.is_empty() {
            eprintln!("kcs-monitor: список символов пуст — пропускаю цикл");
        } else {
            // Первый свип процесса — первичная загрузка истории (глубже),
            // последующие — только свежие бары.
            let bars = if sweep_no == 1 && cfg.bars_first > 0 {
                cfg.bars_first
            } else {
                cfg.bars
            };
            let summary = tokio::select! {
                _ = wait_for_shutdown() => {
                    eprintln!("kcs-monitor: остановка по сигналу");
                    break;
                }
                s = sweep::run(&cfg, &client, &symbols, bars, db_tx.as_ref()) => s,
            };
            eprintln!(
                "kcs-monitor: свип #{sweep_no} ({bars} бар/пару) за {:.1}s: пар ок {}, ошибок {}, свечей {} (из них 429: {})",
                started.elapsed().as_secs_f64(),
                summary.pairs_ok,
                summary.pairs_err,
                summary.candles,
                summary.rate_limited
            );
            // В разовом режиме ошибки важны для cron — возвращаем код 1.
            if cfg.interval_secs == 0 && summary.pairs_err > 0 {
                drop(db_tx);
                tokio::time::sleep(Duration::from_millis(500)).await;
                std::process::exit(1);
            }
        }

        if cfg.interval_secs == 0 {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(cfg.interval_secs)) => {}
            _ = wait_for_shutdown() => {
                eprintln!("kcs-monitor: остановка по сигналу");
                break;
            }
        }
    }

    // Сбрасываем буфер записи и выходим.
    drop(db_tx);
    tokio::time::sleep(Duration::from_millis(500)).await;
    eprintln!("kcs-monitor: остановлен");
    Ok(())
}

/// Ждёт SIGINT (ctrl-c) или SIGTERM (docker stop).
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("не удалось подписаться на SIGTERM");
        let mut sigint = signal(SignalKind::interrupt()).expect("не удалось подписаться на SIGINT");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
