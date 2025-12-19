CREATE OR REFRESH LIVE TABLE coingecko.silver.silver_moeda_diaria
COMMENT "Métricas diárias agregadas por moeda (streaming) a partir da bronze."
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  DATE(timestamp)                               AS data_referencia,
  id                                            AS id_moeda,
  symbol                                        AS simbolo,
  name                                          AS nome_moeda,
  ROUND(AVG(current_price), 2)                  AS preco_medio_usd,
  ROUND(AVG(market_cap), 0)                     AS valor_mercado_medio_usd,
  ROUND(AVG(total_volume), 0)                   AS volume_medio_usd,
  current_timestamp()                           AS ts_processamento
FROM coingecko.bronze.bronze_moeda
GROUP BY
  DATE(timestamp),
  id,
  symbol,
  name;


CREATE OR REFRESH LIVE TABLE coingecko.silver.silver_fato_moeda_diaria
COMMENT "Tabela fato diária consolidada (1 linha por moeda/dia), escolhendo a versão mais recente."
TBLPROPERTIES ("quality" = "silver")
AS
WITH base AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY data_referencia, id_moeda
      ORDER BY ts_processamento DESC
    ) AS rn
  FROM coingecko.silver.silver_moeda_diaria
)
SELECT
  data_referencia,
  id_moeda,
  simbolo,
  nome_moeda,
  preco_medio_usd,
  valor_mercado_medio_usd,
  volume_medio_usd,
  ts_processamento
FROM base
WHERE rn = 1;
