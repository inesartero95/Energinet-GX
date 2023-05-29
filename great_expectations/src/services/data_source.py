
class SynapseDataSource:
    
    def __init__(self, datasource_name: str, connection_string: str):
        self.datasource_name = datasource_name
        self.connection_string = connection_string
        self.yaml_file = f"""
                                name: {self.datasource_name}
                                class_name: Datasource
                                execution_engine:
                                  class_name: SqlAlchemyExecutionEngine
                                  connection_string: {self.connection_string}
                                  create_temp_table: False
                                data_connectors:
                                    default_inferred_data_connector_name:
                                        class_name: InferredAssetSqlDataConnector
                                        include_schema_name: True
        """