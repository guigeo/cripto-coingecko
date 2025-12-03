CREATE OR REFRESH STREAMING LIVE TABLE coingecko.bronze.coingecko_coins
COMMENT "Bronze — extração de criptos a partir de raw no volume"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
    CAST(id AS STRING)           AS id,
    CAST(symbol AS STRING)           AS symbol,
    CAST(name AS STRING)                  AS name,
    CAST(timestamp AS STRING)            AS timestamp,
    CAST(current_price AS STRING)           AS current_price,
    CAST(market_cap AS STRING)       AS market_cap,
    CAST(total_volume AS STRING)    AS total_volume,
    CAST(dt AS STRING)     AS dt,
    current_timestamp()               AS ingested_at
FROM cloud_files(
  "${input_path_raw}",
  "parquet",
  map(
    "header", "true",
    "cloudFiles.inferColumnTypes", "true",
    "cloudFiles.schemaEvolutionMode", "addNewColumns",
    "pathGlobFilter", "*.parquet"
  )
);    