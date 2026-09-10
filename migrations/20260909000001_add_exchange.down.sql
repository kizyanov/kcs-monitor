-- SQLx migration (down): откат мультибиржевого ключа.

-- Возвращаем прежние PK и индекс «по паре», затем удаляем колонку exchange.
ALTER TABLE candles DROP CONSTRAINT candles_pkey;
ALTER TABLE candles ADD PRIMARY KEY (symbol, timeframe, start_ts);

DROP INDEX candles_recent_idx;
CREATE INDEX candles_recent_idx ON candles (symbol, timeframe, start_ts DESC);

ALTER TABLE candles DROP COLUMN exchange;
