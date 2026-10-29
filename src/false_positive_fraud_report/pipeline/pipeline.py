from io_utils.data_handler import DataHandler
from processing.transformations import Transformation

class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """
    def __init__(self, data_handler: DataHandler, data_transformer: Transformation):
        self.data_handler = data_handler
        self.data_transformer = data_transformer

    def run(self, configs):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """

        # Getting variables
        ## Paths configs
        PATH_PAGAMENTO = configs.get("paths",{}).get("pagamentos")
        PATH_PEDIDOS = configs.get("paths",{}).get("pedidos")
        PATH_OUTPUT = configs.get("paths",{}).get("output")
        ## Load Configs
        COMPRESSION_PAGAMENTOS = configs.get("file_options",{}).get("pagamentos_json", {}).get("compression")
        HEADER_PEDIDOS = configs.get("file_options",{}).get("pedidos_csv", {}).get("header")
        SEPARATOR_PEDIDOS = configs.get("file_options",{}).get("pedidos_csv", {}).get("sep")
        COMPRESSION_PEDIDOS = configs.get("file_options",{}).get("pedidos_csv", {}).get("compression")
        ## Load Transformer Configs
        TARGET_YEAR = configs.get("transformer",{}).get("target_year")

        # Loading dataframes
        ## Pedidos
        pedidos = self.data_handler.load_pedidos(
            path = PATH_PEDIDOS, 
            header = HEADER_PEDIDOS, 
            sep = SEPARATOR_PEDIDOS, 
            compression = COMPRESSION_PEDIDOS)
        ## Pagamentos
        pagamentos = self.data_handler.load_pagamentos(
            path = PATH_PAGAMENTO, 
            compression = COMPRESSION_PAGAMENTOS
        )

        # Criando a coluna de valor total
        pedidos = self.data_transformer.add_total_value(orders_df = pedidos)

        # Filtrando pedidos de 2025
        pedidos_filtrados = self.data_transformer.year_orders_filter(target_year = TARGET_YEAR, orders_df = pedidos)

        # Filtrando dataset de pagamentos
        pagamentos_filtrados = self.data_transformer.false_negative_payments_filter(payments_df = pagamentos)

        # Unificando os dados
        join_df = self.data_transformer.join_information(payments_df = pagamentos_filtrados, orders_df = pedidos_filtrados)

        # Gerando o relatório
        relatorio = self.data_transformer.priorization(dataframe = join_df)

        # Escrevendo o relatório na pasta de output
        self.data_handler.write_parquet(dataframe=relatorio, path=PATH_OUTPUT)
