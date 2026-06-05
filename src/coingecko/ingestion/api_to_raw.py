"""
Ingestão CoinGecko → Volume raw.

Uso:
  # Incremental (padrão — detecta última data carregada):
  python api_to_raw.py

  # Backfill com intervalo explícito:
  python api_to_raw.py --start-date 2025-10-01 --end-date 2025-10-31
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta, date

import pandas as pd
from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))
from coingecko.utils.api import CoinGeckoClient

OUTPUT_PATH = "/Volumes/coingecko/raw/raw"
TOP_N = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_spark():
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    from databricks.connect import DatabricksSession
    return DatabricksSession.builder.serverless().getOrCreate()


def get_last_loaded_date(spark, output_path: str) -> date | None:
    try:
        result = spark.read.parquet(output_path).agg(F.max("dt")).collect()[0][0]
        return result
    except Exception:
        return None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="CoinGecko API → Raw Volume")
    parser.add_argument("--start-date", type=str, help="Data início (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Data fim (YYYY-MM-DD). Padrão: D-2")
    return parser.parse_args(argv)


def resolve_date_range(args, spark) -> tuple[date, date]:
    today = datetime.now().date()
    end_date = (
        datetime.strptime(args.end_date, "%Y-%m-%d").date()
        if args.end_date
        else today - timedelta(days=2)
    )

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    else:
        last_loaded = get_last_loaded_date(spark, OUTPUT_PATH)
        if last_loaded is None:
            start_date = end_date
            log.info("Nenhum dado existente. Iniciando do zero a partir de D-2.")
        else:
            start_date = last_loaded + timedelta(days=1)
            log.info(f"Última data carregada: {last_loaded}. Iniciando de {start_date}.")

    return start_date, end_date


def fetch_and_collect(client: CoinGeckoClient, coin_meta: dict, start_date: date, end_date: date) -> list[dict]:
    all_records = []

    for single_date in pd.date_range(start_date, end_date):
        dt = single_date.date()
        dt_from = int(datetime.combine(dt, datetime.min.time()).timestamp())
        dt_to = int(datetime.combine(dt, datetime.max.time()).timestamp())

        log.info(f"Processando {dt} ({len(coin_meta)} moedas)...")

        for coin_id, meta in coin_meta.items():
            records = client.get_coin_history(coin_id, dt_from, dt_to)
            for r in records:
                r["id"] = coin_id
                r["symbol"] = meta["symbol"]
                r["name"] = meta["name"]
            all_records.extend(records)

        log.info(f"  → {dt} concluído. Total acumulado: {len(all_records)} registros.")

    return all_records


def save_to_volume(spark, records: list[dict], output_path: str) -> None:
    df = pd.DataFrame(records)
    sdf = spark.createDataFrame(df)
    sdf = (
        sdf.withColumn("dt", F.to_date("timestamp"))
           .withColumn("mes", F.date_format("dt", "yyyy-MM"))
    )
    (
        sdf.write
           .mode("append")
           .partitionBy("mes", "dt")
           .parquet(output_path)
    )
    log.info(f"Salvo: {len(records)} registros em {output_path}")


def main(argv=None):
    args = parse_args(argv)
    spark = get_spark()

    start_date, end_date = resolve_date_range(args, spark)

    if start_date > end_date:
        log.info("Dados já atualizados. Nada a processar.")
        return

    log.info(f"Intervalo: {start_date} → {end_date}")

    client = CoinGeckoClient()

    log.info(f"Buscando top {TOP_N} moedas na CoinGecko...")
    top_coins = client.get_top_coins(TOP_N)
    coin_meta = {c["id"]: {"symbol": c["symbol"], "name": c["name"]} for c in top_coins}
    log.info(f"Moedas obtidas: {len(coin_meta)}")

    records = fetch_and_collect(client, coin_meta, start_date, end_date)

    if not records:
        log.warning("Nenhum dado retornado pela API. Encerrando.")
        return

    save_to_volume(spark, records, OUTPUT_PATH)


if __name__ == "__main__":
    main()
