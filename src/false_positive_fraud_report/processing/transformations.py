from pyspark.sql import DataFrame
from pyspark.sql.functions import col, year, asc, desc

class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def add_total_value(self, orders_df: DataFrame) -> DataFrame:
        "Creating a column that calculate the product between quantity and unit value"
        return(
            orders_df
            .withColumn(
                "valor_total", 
                col("valor_unitario") * col("quantidade")
            )
        )
    
    def year_orders_filter(self, target_year: int, orders_df: DataFrame) -> DataFrame:
        "Filtering the orders for a specific year"
        return(
            orders_df
            .filter(year(col("data_criacao")) == target_year)
        )
    
    def false_negative_payments_filter(self, payments_df: DataFrame) -> DataFrame:
        "Filtering cases where the order was not identified as fraud, but it had a high value and it was suspicious"
        return(
            payments_df
            .filter(col("status") == False)
            .filter(col("avaliacao_fraude.fraude") == False)
        )
    
    def join_information(self, payments_df: DataFrame, orders_df: DataFrame) -> DataFrame:
        "Joining the payments and orders information in one dataframe and select the main columns"
        return(
            payments_df.alias("p")
            .join(
                orders_df.alias("o"),
                (col("p.id_pedido") == col("o.id_pedido")),
                how = "inner"
            )
            .select(
                col("p.id_pedido"),
                col("o.uf"),
                col("p.forma_pagamento"),
                col("o.valor_total"),
                col("o.data_criacao")
            )
        )
    
    def priorization(self, dataframe: DataFrame) -> DataFrame:
        "Prioritizing the orders by UF, payment type and total value"
        return (
            dataframe
            .orderBy(
                asc(col("uf")), 
                asc(col("forma_pagamento")), 
                desc(col("valor_total"))
            )
        )