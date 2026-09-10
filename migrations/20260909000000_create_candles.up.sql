-- SQLx migration (up): таблица свечей KuCoin для kcs-monitor.
--
-- Применение:  sqlx migrate run
-- Откат:        sqlx migrate revert
--
-- Модель данных:
--   * каждая строка — одна свеча (пара + таймфрейм + время начала);
--   * WS шлёт обновления формирующейся свечи многократно, поэтому запись
--     идёт через UPSERT по первичному ключу (см. пример внизу);
--   * время начала свечи хранится как BIGINT — unix-секунды (UTC), ровно
--     как их отдаёт биржа (поле start в строках kcs-monitor). Это убирает
--     любые timezone-преобразования при записи; при чтении timestamptz =
--     to_timestamp(start_ts).

CREATE TABLE candles (
    symbol      text        NOT NULL,                -- например 'BTC-USDT'
    timeframe   text        NOT NULL,                -- '1hour', '4hour', '1day', '1week' (KuCoin: 1min..1week)
    start_ts    bigint      NOT NULL,                -- начало свечи, unix-секунды (UTC)
    open        numeric     NOT NULL,
    high        numeric     NOT NULL,
    low         numeric     NOT NULL,
    close       numeric     NOT NULL,
    volume      numeric     NOT NULL,
    turnover    numeric     NOT NULL DEFAULT 0,
    update_time timestamptz NOT NULL DEFAULT now(),  -- когда пришло последнее обновление

    PRIMARY KEY (symbol, timeframe, start_ts)
);

-- Выборки «по времени» (аналитика по всем парам за период):
CREATE INDEX candles_start_ts_idx ON candles (start_ts);

-- Самые свежие бары по паре (ускоренный DESC-скан вместо реверса PK):
CREATE INDEX candles_recent_idx ON candles (symbol, timeframe, start_ts DESC);

COMMENT ON TABLE candles IS
    'Свечи KuCoin, получаемые kcs-monitor по WebSocket';
COMMENT ON COLUMN candles.start_ts IS
    'Время начала свечи в unix-секундах (UTC), как в payload биржи; timestamptz = to_timestamp(start_ts)';
