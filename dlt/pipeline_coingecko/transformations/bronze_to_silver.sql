CREATE OR REFRESH LIVE TABLE coingecko.silver.silver_moeda_diaria
CONSTRAINT valid_data  EXPECT (data_referencia IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_preco EXPECT (preco_medio_usd > 0)         ON VIOLATION DROP ROW
COMMENT "Métricas diárias por moeda — uma linha por (moeda, dia) com preço médio, min e max."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  DATE(timestamp)                              AS data_referencia,
  id                                           AS id_moeda,
  symbol                                       AS simbolo,
  name                                         AS nome_moeda,
  ROUND(AVG(current_price), 2)                 AS preco_medio_usd,
  ROUND(MAX(current_price), 2)                 AS preco_max_usd,
  ROUND(MIN(current_price), 2)                 AS preco_min_usd,
  CAST(ROUND(AVG(market_cap), 0) AS BIGINT)    AS valor_mercado_medio_usd,
  CAST(ROUND(AVG(total_volume), 0) AS BIGINT)  AS volume_medio_usd,
  current_timestamp()                          AS ts_processamento
FROM coingecko.bronze.bronze_moeda
GROUP BY
  DATE(timestamp),
  id,
  symbol,
  name;
