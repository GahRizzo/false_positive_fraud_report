from session.spark_session import SparkSessionManager
from config.settings import load_config
from io_utils.data_handler import DataHandler
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

pedidos.printSchema()
pagamentos.printSchema()

pedidos.show()
pagamentos.show()