-- SQLx migration (down): удаление таблицы свечей KuCoin.
--
-- Индексы и комментарии удаляются вместе с таблицей.

DROP TABLE candles;
