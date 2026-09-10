//! Конфигурация приложения (переменные окружения).

/// Настройки, читаемые из окружения.
#[derive(Debug, Clone)]
pub struct Config {
    /// Базовый URL REST API биржи.
    pub api_base: String,
    /// Биржа-источник для ключа свечей в БД (kucoin, binance, ...).
    pub exchange: String,
    /// Таймфреймы свечей (KuCoin): 1min..1week.
    pub kline_intervals: Vec<String>,
    /// Сколько последних ЗАКРЫТЫХ баров тянуть в обычном свипе.
    pub bars: usize,
    /// Глубина первого свипа процесса (первичная загрузка истории).
    /// 0 = использовать `bars` как обычно.
    pub bars_first: usize,
    /// Максимум одновременных REST-запросов.
    pub concurrency: usize,
    /// Пауза между свипами, секунды. 0 = один проход и выход (для cron).
    pub interval_secs: u64,
    /// URL PostgreSQL; None = свечи выводятся в stdout, без записи.
    pub db_url: Option<String>,
    /// Отладочное ограничение списка пар ("BTC-USDT,ETH-USDT").
    pub symbols_override: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        let env = |key: &str| std::env::var(key).ok().filter(|v| !v.is_empty());
        Config {
            api_base: env("KCS_API_BASE")
                .unwrap_or_else(|| crate::kucoin::DEFAULT_API_BASE.to_string()),
            exchange: env("KCS_EXCHANGE").unwrap_or_else(|| "kucoin".to_string()),
            // KCS_KLINE_INTERVALS="1hour,4hour,1day,1week";
            // одиночный KCS_KLINE_INTERVAL принимается для совместимости.
            kline_intervals: env("KCS_KLINE_INTERVALS")
                .or_else(|| env("KCS_KLINE_INTERVAL"))
                .map(|s| {
                    s.split(',')
                        .map(|x| x.trim().to_string())
                        .filter(|x| !x.is_empty())
                        .collect()
                })
                .filter(|v: &Vec<String>| !v.is_empty())
                .unwrap_or_else(|| {
                    vec![
                        "1hour".to_string(),
                        "4hour".to_string(),
                        "1day".to_string(),
                        "1week".to_string(),
                    ]
                }),
            bars: env("KCS_BARS")
                .or_else(|| env("KCS_BACKFILL_BARS"))
                .and_then(|v| v.parse().ok())
                .unwrap_or(3)
                .max(1),
            bars_first: env("KCS_BARS_FIRST")
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            concurrency: env("KCS_CONCURRENCY")
                .or_else(|| env("KCS_BACKFILL_CONCURRENCY"))
                .and_then(|v| v.parse().ok())
                .unwrap_or(8)
                .max(1),
            interval_secs: env("KCS_INTERVAL_SECONDS")
                .and_then(|v| v.parse().ok())
                .unwrap_or(600),
            db_url: env("KCS_DATABASE_URL").or_else(|| env("DATABASE_URL")),
            symbols_override: env("KCS_SYMBOLS"),
        }
    }
}
