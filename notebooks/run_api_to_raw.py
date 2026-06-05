# Databricks notebook source
# Runner para o job de ingestão. Lógica em src/coingecko/ingestion/api_to_raw.py

# COMMAND ----------
%pip install requests

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys

# Deriva o usuário atual via SQL (compatível com Spark Connect/serverless)
username = spark.sql("SELECT current_user()").collect()[0][0]
src_path = f"/Workspace/Users/{username}/.bundle/cripto-coingecko/dev/files/src"
sys.path.insert(0, src_path)

from coingecko.ingestion.api_to_raw import main

# argv=[] evita que o Databricks injete argumentos desconhecidos no argparse
main(argv=[])
