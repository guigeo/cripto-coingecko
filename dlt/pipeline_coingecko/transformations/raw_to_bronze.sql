CREATE OR REFRESH STREAMING LIVE TABLE coingecko.bronze.bronze_moeda (
  CONSTRAINT valid_id        EXPECT (id IS NOT NULL)        ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price     EXPECT (current_price > 0)     ON VIOLATION DROP ROW
)
COMMENT "Bronze — extração de criptos a partir de raw no volume"
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT
    CAST(id            AS STRING)    AS id,
    CAST(symbol        AS STRING)    AS symbol,
    CAST(name          AS STRING)    AS name,
    CAST(timestamp     AS TIMESTAMP) AS timestamp,
    CAST(current_price AS DOUBLE)    AS current_price,
    CAST(market_cap    AS DOUBLE)    AS market_cap,
    CAST(total_volume  AS DOUBLE)    AS total_volume,
    CAST(dt            AS DATE)      AS dt,
    current_timestamp()              AS ingested_at
FROM cloud_files(
  "${input_path_raw}",
  "parquet",
  map(
    "cloudFiles.inferColumnTypes",    "true",
    "cloudFiles.schemaEvolutionMode", "addNewColumns",
    "pathGlobFilter",                 "*.parquet"
  )
);
