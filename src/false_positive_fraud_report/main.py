from session.spark_session import SparkSessionManager
from config.settings import load_config

# Loading setup configs
configs = load_config(path = "./config/settings.yaml")

# Getting variables
app_name = configs.get("spark",{}).get("app_name")

# Start spark session
spark = SparkSessionManager.get_spark_session(app_name = app_name)