CREATE OR REFRESH LIVE TABLE coingecko.gold.gold_ranking_moedas_diario
COMMENT "Ranking diário de moedas por valor de mercado e por volume (top 100)."
TBLPROPERTIES ("quality" = "gold")
AS
WITH base AS (
  SELECT
    data_referencia,
    id_moeda,
    simbolo,
    nome_moeda,
    preco_medio_usd,
    valor_mercado_medio_usd,
    volume_medio_usd,

    dense_rank() OVER (
      PARTITION BY data_referencia
      ORDER BY valor_mercado_medio_usd DESC
    ) AS rank_valor_mercado,

    dense_rank() OVER (
      PARTITION BY data_referencia
      ORDER BY volume_medio_usd DESC
    ) AS rank_volume
  FROM coingecko.silver.silver_fato_moeda_diaria
)
SELECT *
FROM base
WHERE rank_valor_mercado <= 100 OR rank_volume <= 100;

CREATE OR REFRESH LIVE TABLE coingecko.gold.gold_variacao_preco_d1
COMMENT "Variação do preço médio (USD) vs dia anterior por moeda."
TBLPROPERTIES ("quality" = "gold")
AS
WITH hoje AS (
  SELECT
    data_referencia,
    id_moeda,
    simbolo,
    nome_moeda,
    preco_medio_usd
  FROM coingecko.silver.silver_fato_moeda_diaria
),
ontem AS (
  SELECT
    data_referencia AS data_ontem,
    id_moeda,
    preco_medio_usd AS preco_medio_ontem_usd
  FROM coingecko.silver.silver_fato_moeda_diaria
)
SELECT
  h.data_referencia,
  h.id_moeda,
  h.simbolo,
  h.nome_moeda,

  h.preco_medio_usd,
  o.preco_medio_ontem_usd,

  ROUND(h.preco_medio_usd - o.preco_medio_ontem_usd, 2) AS variacao_abs_d1_usd,

  ROUND(
    (h.preco_medio_usd - o.preco_medio_ontem_usd) / NULLIF(o.preco_medio_ontem_usd, 0) * 100,
    2
  ) AS variacao_pct_d1
FROM hoje h
LEFT JOIN ontem o
  ON h.id_moeda = o.id_moeda
 AND o.data_ontem = date_add(h.data_referencia, -1);


CREATE OR REFRESH LIVE TABLE coingecko.gold.gold_resumo_mercado_diario
COMMENT "Resumo diário do mercado (com base nas moedas ingeridas) para painéis e KPIs."
TBLPROPERTIES ("quality" = "gold")
AS
WITH base AS (
  SELECT
    data_referencia,
    id_moeda,
    simbolo,
    nome_moeda,
    preco_medio_usd,
    valor_mercado_medio_usd,
    volume_medio_usd
  FROM coingecko.silver.silver_fato_moeda_diaria
),

agregado AS (
  SELECT
    data_referencia,

    COUNT(*)                                         AS qtd_moedas,
    ROUND(SUM(valor_mercado_medio_usd), 0)           AS valor_mercado_total_usd,
    ROUND(SUM(volume_medio_usd), 0)                  AS volume_total_usd,

    ROUND(AVG(preco_medio_usd), 2)                   AS preco_medio_geral_usd,
    ROUND(percentile_approx(preco_medio_usd, 0.5), 2) AS preco_mediano_geral_usd,

    ROUND(AVG(valor_mercado_medio_usd), 0)           AS valor_mercado_medio_por_moeda_usd,
    ROUND(AVG(volume_medio_usd), 0)                  AS volume_medio_por_moeda_usd
  FROM base
  GROUP BY data_referencia
),

top_market_cap AS (
  SELECT
    data_referencia,
    first(id_moeda)   AS top_valor_mercado_id_moeda,
    first(simbolo)    AS top_valor_mercado_simbolo,
    first(nome_moeda) AS top_valor_mercado_nome,
    first(valor_mercado_medio_usd) AS top_valor_mercado_usd
  FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY data_referencia
        ORDER BY valor_mercado_medio_usd DESC
      ) AS rn
    FROM base
  ) t
  WHERE rn = 1
  GROUP BY data_referencia
),

top_volume AS (
  SELECT
    data_referencia,
    first(id_moeda)   AS top_volume_id_moeda,
    first(simbolo)    AS top_volume_simbolo,
    first(nome_moeda) AS top_volume_nome,
    first(volume_medio_usd) AS top_volume_usd
  FROM (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY data_referencia
        ORDER BY volume_medio_usd DESC
      ) AS rn
    FROM base
  ) t
  WHERE rn = 1
  GROUP BY data_referencia
)

SELECT
  a.data_referencia,

  a.qtd_moedas,
  a.valor_mercado_total_usd,
  a.volume_total_usd,

  a.preco_medio_geral_usd,
  a.preco_mediano_geral_usd,
  a.valor_mercado_medio_por_moeda_usd,
  a.volume_medio_por_moeda_usd,

  tm.top_valor_mercado_id_moeda,
  tm.top_valor_mercado_simbolo,
  tm.top_valor_mercado_nome,
  tm.top_valor_mercado_usd,

  tv.top_volume_id_moeda,
  tv.top_volume_simbolo,
  tv.top_volume_nome,
  tv.top_volume_usd

FROM agregado a
LEFT JOIN top_market_cap tm
  ON a.data_referencia = tm.data_referencia
LEFT JOIN top_volume tv
  ON a.data_referencia = tv.data_referencia;

CREATE OR REFRESH LIVE TABLE coingecko.gold.gold_resumo_moeda
COMMENT "Resumo histórico por moeda + valores do último dia (market cap/volume/preço) para rankings corretos."
TBLPROPERTIES ("quality" = "gold")
AS
WITH base AS (
  SELECT
    data_referencia,
    id_moeda,
    simbolo,
    nome_moeda,
    preco_medio_usd,
    valor_mercado_medio_usd,
    volume_medio_usd
  FROM coingecko.silver.silver_fato_moeda_diaria
),

estatisticas AS (
  SELECT
    id_moeda,
    max(simbolo) AS simbolo,
    max(nome_moeda) AS nome_moeda,

    MIN(data_referencia) AS data_primeiro_dia,
    MAX(data_referencia) AS data_ultimo_dia,

    COUNT(*) AS qtd_dias_com_dados,

    ROUND(AVG(preco_medio_usd), 2) AS preco_medio_historico_usd,
    ROUND(MIN(preco_medio_usd), 2) AS preco_min_historico_usd,
    ROUND(MAX(preco_medio_usd), 2) AS preco_max_historico_usd,

    ROUND(AVG(valor_mercado_medio_usd), 0) AS valor_mercado_medio_historico_usd,
    ROUND(AVG(volume_medio_usd), 0)        AS volume_medio_historico_usd
  FROM base
  GROUP BY id_moeda
),

primeiro_dia AS (
  SELECT
    id_moeda,
    data_referencia AS data_primeiro_dia_real,
    preco_medio_usd AS preco_primeiro_dia_usd
  FROM (
    SELECT
      b.*,
      row_number() OVER (
        PARTITION BY b.id_moeda
        ORDER BY b.data_referencia ASC
      ) AS rn
    FROM base b
  ) t
  WHERE rn = 1
),

ultimo_dia AS (
  SELECT
    id_moeda,
    data_referencia AS data_ultimo_dia_real,
    preco_medio_usd AS preco_ultimo_dia_usd,
    valor_mercado_medio_usd AS valor_mercado_ultimo_dia_usd,
    volume_medio_usd        AS volume_ultimo_dia_usd
  FROM (
    SELECT
      b.*,
      row_number() OVER (
        PARTITION BY b.id_moeda
        ORDER BY b.data_referencia DESC
      ) AS rn
    FROM base b
  ) t
  WHERE rn = 1
)

SELECT
  e.id_moeda,
  e.simbolo,
  e.nome_moeda,

  e.data_primeiro_dia,
  e.data_ultimo_dia,
  e.qtd_dias_com_dados,

  e.preco_medio_historico_usd,
  e.preco_min_historico_usd,
  e.preco_max_historico_usd,

  p.preco_primeiro_dia_usd,
  u.preco_ultimo_dia_usd,

  ROUND(
    (u.preco_ultimo_dia_usd - p.preco_primeiro_dia_usd) / NULLIF(p.preco_primeiro_dia_usd, 0) * 100,
    2
  ) AS variacao_percentual_total,

  e.valor_mercado_medio_historico_usd,
  e.volume_medio_historico_usd,

  -- ✅ NOVOS: valores do último dia (para ranking "atual")
  u.valor_mercado_ultimo_dia_usd,
  u.volume_ultimo_dia_usd,

  -- opcionais pra BI (mais legível)
  ROUND(e.valor_mercado_medio_historico_usd / 1e9, 2) AS valor_mercado_medio_historico_bi_usd,
  ROUND(e.volume_medio_historico_usd / 1e9, 2)        AS volume_medio_historico_bi_usd,
  ROUND(u.valor_mercado_ultimo_dia_usd / 1e9, 2)       AS valor_mercado_ultimo_dia_bi_usd,
  ROUND(u.volume_ultimo_dia_usd / 1e9, 2)              AS volume_ultimo_dia_bi_usd

FROM estatisticas e
LEFT JOIN primeiro_dia p ON e.id_moeda = p.id_moeda
LEFT JOIN ultimo_dia   u ON e.id_moeda = u.id_moeda;
