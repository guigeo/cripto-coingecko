# cripto-coingecko — Instruções para Claude

## Contexto do Projeto

Pipeline de dados de criptomoedas no **Databricks Free Edition** (serverless only).
Coleta top 50 criptos da CoinGecko API e processa via arquitetura medallion.

**Workspace:** `https://dbc-f1990488-7bac.cloud.databricks.com`
**Perfil CLI:** `My Free Edition`
**Catálogo Unity Catalog:** `coingecko`

---

## Estrutura

```
src/coingecko/ingestion/api_to_raw.py   # Script de ingestão principal
src/coingecko/utils/api.py              # Cliente CoinGecko
dlt/pipeline_coingecko/transformations/ # SQLs do pipeline DLT
resources/jobs.yml                      # Job agendado
resources/pipelines.yml                 # Pipeline DLT declarado no bundle
notebooks/run_api_to_raw.py             # Runner do job (notebook Databricks)
notebooks/exploration.ipynb             # Exploração local (Databricks Connect)
```

---

## Comandos Essenciais

```bash
# Validar bundle
databricks bundle validate

# Deploy
databricks bundle deploy

# Rodar ingestão (job)
databricks bundle run coingecko_ingestion

# Rodar pipeline DLT
databricks bundle run coingecko_dlt

# Full refresh do pipeline
databricks bundle run coingecko_dlt --full-refresh-all

# Verificar autenticação
databricks auth profiles
```

---

## Restrições Importantes

- **Serverless only** — Free Edition não suporta clusters clássicos. `spark_python_task` com `client: "1"` não funciona. Use `notebook_task`.
- **Sem `spark` global local** — Usar `DatabricksSession.builder.serverless().profile("My Free Edition").getOrCreate()` nos notebooks locais.
- **Coluna `dt` não existe nos arquivos parquet** — É partição de diretório (`partitionBy("mes", "dt")`). O Auto Loader não a expõe como coluna. Derivar de `timestamp` com `DATE(timestamp)`.
- **Pipelines DLT** — Tabelas DLT são "owned" por um pipeline. Se trocar de pipeline ID (novo deploy), dropar as tabelas antigas antes de fazer full refresh.
- **CONSTRAINT no DLT SQL** — Deve ficar dentro de parênteses após o nome da tabela: `CREATE ... TABLE nome (...constraints...) AS`.

---

## Fluxo de Dados

```
CoinGecko API → /Volumes/coingecko/raw/raw (parquet)
    → bronze.bronze_moeda (streaming, Auto Loader)
    → silver.silver_moeda_diaria (agregado diário: avg/max/min price)
    → gold.gold_ranking_moedas_diario
    → gold.gold_variacao_preco_d1  (usa LAG, não self-join)
    → gold.gold_resumo_mercado_diario
    → gold.gold_resumo_moeda
```

## Lógica de Incrementalidade

`api_to_raw.py` detecta `MAX(dt)` no volume raw e processa apenas dias novos até D-2.
Para backfill: `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`.
O runner de notebook passa `argv=[]` para evitar conflito com `sys.argv` do Databricks.

## Rate Limit CoinGecko (Free Tier)

- 2s de sleep entre moedas
- Retry exponencial: 10s × tentativa em caso de 429
- ~28 min para processar 1 dia com 50 moedas

---

## Consultas Úteis

```sql
-- Últimos dados disponíveis por camada
SELECT MAX(DATE(timestamp)) FROM coingecko.bronze.bronze_moeda;
SELECT MAX(data_referencia) FROM coingecko.silver.silver_moeda_diaria;
SELECT MAX(data_referencia) FROM coingecko.gold.gold_resumo_mercado_diario;

-- Ranking atual
SELECT * FROM coingecko.gold.gold_ranking_moedas_diario
WHERE data_referencia = (SELECT MAX(data_referencia) FROM coingecko.gold.gold_ranking_moedas_diario)
ORDER BY rank_valor_mercado;

-- Variação D1 mais recente
SELECT * FROM coingecko.gold.gold_variacao_preco_d1
WHERE data_referencia = (SELECT MAX(data_referencia) FROM coingecko.gold.gold_variacao_preco_d1)
ORDER BY variacao_pct_d1 DESC;
```
