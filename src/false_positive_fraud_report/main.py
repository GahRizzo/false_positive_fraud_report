from session.spark_session import SparkSessionManager
from config.settings import load_config
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

from pyspark.sql.functions import *


# Loading setup configs
configs = load_config(path = "./config/settings.yaml")

# Getting variables
## Spark app name
APP_NAME = configs.get("spark",{}).get("app_name")
## Paths configs
PATH_PAGAMENTO = configs.get("paths",{}).get("pagamentos")
PATH_PEDIDOS = configs.get("paths",{}).get("pedidos")
PATH_OUTPUT = configs.get("paths",{}).get("output")
## Load Configs
COMPRESSION_PAGAMENTOS = configs.get("file_options",{}).get("pagamentos_json", {}).get("compression")
HEADER_PEDIDOS = configs.get("file_options",{}).get("pedidos_csv", {}).get("header")
SEPARATOR_PEDIDOS = configs.get("file_options",{}).get("pedidos_csv", {}).get("sep")
COMPRESSION_PEDIDOS = configs.get("file_options",{}).get("pedidos_csv", {}).get("compression")

# Start spark session
spark = SparkSessionManager.get_spark_session(app_name = APP_NAME)

# Instance data handler
data_handler = DataHandler(spark = spark)

# Instance data transformer
data_transformer = Transformation()

# Loading dataframes
## Pedidos
pedidos = data_handler.load_pedidos(
    path = PATH_PEDIDOS, 
    header = HEADER_PEDIDOS, 
    sep = SEPARATOR_PEDIDOS, 
    compression = COMPRESSION_PEDIDOS)
## Pagamentos
pagamentos = data_handler.load_pagamentos(
    path = PATH_PAGAMENTO, 
    compression = COMPRESSION_PAGAMENTOS
)

# Criando a coluna de valor total
pedidos = data_transformer.add_total_value(orders_df = pedidos)

# Filtrando pedidos de 2025
pedidos_filtrados = data_transformer.year_orders_filter(target_year = 2025, orders_df = pedidos)

# Filtrando dataset de pagamentos
pagamentos_filtrados = data_transformer.false_negative_payments_filter(payments_df = pagamentos)

# Unificando os dados
join_df = data_transformer.join_information(payments_df = pagamentos_filtrados, orders_df = pedidos_filtrados)

# Gerando o relatório
relatorio = data_transformer.priorization(dataframe = join_df)

spark.stop()