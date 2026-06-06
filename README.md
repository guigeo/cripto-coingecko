# cripto-coingecko

Pipeline de dados de criptomoedas usando a API pública do CoinGecko, armazenado e processado no Databricks Free Edition com arquitetura medallion (raw → bronze → silver → gold).

---

## Visão Geral

Coleta diária das top 50 criptomoedas por market cap, com histórico desde outubro/2025. Os dados são ingeridos via API REST, armazenados em Volume parquet, processados por um pipeline DLT e disponibilizados em tabelas gold para análise e dashboards.

## Arquitetura

```
CoinGecko API
     │
     ▼
[ api_to_raw.py ]  ── Job agendado diário (8h BRT)
     │
     ▼
/Volumes/coingecko/raw/raw  (parquet particionado por mes/dt)
     │
     ▼
[ DLT Pipeline ]
     │
     ├── bronze.bronze_moeda          (streaming, Auto Loader)
     │
     ├── silver.silver_moeda_diaria   (agregado diário por moeda)
     │
     └── gold.gold_*                  (tabelas analíticas)
```

### Tabelas Gold

| Tabela | Descrição |
|---|---|
| `gold_ranking_moedas_diario` | Ranking diário por market cap e volume |
| `gold_variacao_preco_d1` | Variação de preço vs dia anterior (LAG) |
| `gold_resumo_mercado_diario` | KPIs agregados do mercado por dia |
| `gold_resumo_moeda` | Histórico completo por moeda com variação total |

---

## Estrutura do Projeto

```
cripto-coingecko/
├── databricks.yml              # Bundle DAB principal
├── pyproject.toml              # Dependências Python (uv)
│
├── src/coingecko/
│   ├── ingestion/
│   │   └── api_to_raw.py       # Script de ingestão (produção)
│   └── utils/
│       └── api.py              # Cliente CoinGecko com retry/backoff
│
├── dlt/pipeline_coingecko/transformations/
│   ├── raw_to_bronze.sql       # Auto Loader → bronze (streaming)
│   ├── bronze_to_silver.sql    # Agregação diária por moeda
│   └── silver_to_gold.sql      # Tabelas analíticas
│
├── resources/
│   ├── jobs.yml                # Job de ingestão agendado
│   └── pipelines.yml           # Pipeline DLT
│
├── notebooks/
│   ├── run_api_to_raw.py       # Runner do job (notebook Databricks)
│   └── exploration.ipynb       # Exploração ad-hoc (Databricks Connect)
│
├── dash/
│   └── Dash Coingecko.lvdash.json  # Dashboard AI/BI (3 páginas)
│
└── .databricks-resources.json      # IDs de recursos deployados no workspace
```

---

## Setup Local

### Pré-requisitos

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) autenticado

### Instalação

```bash
# Criar ambiente virtual
uv venv --python 3.11
source .venv/bin/activate

# Instalar dependências
uv pip install -e ".[dev]"
```

### Autenticação Databricks

```bash
databricks auth login --host https://dbc-f1990488-7bac.cloud.databricks.com
```

Verifique o perfil `My Free Edition` em `~/.databrickscfg`.

---

## Uso

### Rodar localmente (exploração)

```python
# notebooks/exploration.ipynb
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.serverless().profile("My Free Edition").getOrCreate()

spark.sql("SELECT * FROM coingecko.gold.gold_resumo_mercado_diario ORDER BY data_referencia DESC LIMIT 10").toPandas()
```

### Backfill de datas específicas

```bash
# Via bundle (produção)
databricks bundle run coingecko_ingestion \
  --python-named-params "start_date=2025-10-01,end_date=2025-10-31"
```

### Deploy completo

```bash
databricks bundle deploy
databricks bundle run coingecko_dlt --full-refresh-all
```

---

## Pipeline DLT

### Qualidade de dados (EXPECT constraints)

| Camada | Constraint | Ação |
|---|---|---|
| bronze | `id IS NOT NULL` | DROP ROW |
| bronze | `timestamp IS NOT NULL` | DROP ROW |
| bronze | `current_price > 0` | DROP ROW |
| silver | `data_referencia IS NOT NULL` | DROP ROW |
| silver | `preco_medio_usd > 0` | DROP ROW |
| gold | `data_referencia IS NOT NULL` | DROP ROW |

### Executar pipeline manualmente

```bash
# Incremental
databricks bundle run coingecko_dlt

# Full refresh (rebuild completo)
databricks bundle run coingecko_dlt --full-refresh-all
```

---

## Dashboard AI/BI

**URL:** `https://dbc-f1990488-7bac.cloud.databricks.com/sql/dashboardsv3/01f1613ceaad1ae895c7aaeec000bc58`

3 páginas com dados das tabelas gold:

| Página | Conteúdo |
|---|---|
| **Mercado** | KPIs (market cap, volume 24h, dominância BTC) + séries 90 dias + top 10 |
| **Performance Diária** | Top 8 altas e quedas D1 + tabela completa de variação |
| **Análise Histórica** | Maiores valorizações/quedas desde out/2025 + detalhe por moeda |

Para re-deployar o dashboard via MCP:
```bash
# Editar dash/Dash Coingecko.lvdash.json e usar manage_dashboard(action="create_or_update")
# O ID do dashboard deployado fica em .databricks-resources.json
```

---

## Dados

- **Fonte:** [CoinGecko API v3](https://www.coingecko.com/en/api) (free tier)
- **Cobertura:** Top 50 criptomoedas por market cap
- **Granularidade:** Horária na raw, diária na silver/gold
- **Histórico:** Outubro/2025 → presente
- **Atualização:** Diária às 8h (horário Brasília), dados com defasagem D-2
