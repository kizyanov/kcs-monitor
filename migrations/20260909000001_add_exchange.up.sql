-- SQLx migration (up): добавляем биржу в ключ свечей.
--
-- Таблица становится мультибиржевой: ключ (exchange, symbol, timeframe, start_ts).
-- Существующие строки (только KuCoin) получают exchange = 'kucoin'.

ALTER TABLE candles ADD COLUMN exchange text NOT NULL DEFAULT 'kucoin';

-- Старый PK (symbol, timeframe, start_ts) заменяем на мультибиржевой.
ALTER TABLE candles DROP CONSTRAINT candles_pkey;
ALTER TABLE candles ADD PRIMARY KEY (exchange, symbol, timeframe, start_ts);

-- Индекс «последние бары по паре» — теперь с учётом биржи.
DROP INDEX candles_recent_idx;
CREATE INDEX candles_recent_idx ON candles (exchange, symbol, timeframe, start_ts DESC);

-- Значение по умолчанию больше не нужно: приложение всегда пишет биржу явно.
ALTER TABLE candles ALTER COLUMN exchange DROP DEFAULT;

COMMENT ON COLUMN candles.exchange IS 'Биржа-источник, например kucoin';
