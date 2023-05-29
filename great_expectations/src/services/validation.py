from typing import Protocol

class SimpleCheckpoint:

    def __init__(self, checkpoint_name: str, suite_name: str, data_asset_name: str, datasource_name: str) -> None:
        self.checkpoint_config = f"""
                              name: {checkpoint_name}
                              config_version: 1.0
                              class_name: SimpleCheckpoint
                              run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
                              validations:
                                - batch_request:
                                    datasource_name: {datasource_name}
                                    data_connector_name: default_inferred_data_connector_name
                                    data_asset_name: {data_asset_name}
                                    data_connector_query:
                                      index: -1
                                  expectation_suite_name: {suite_name}
                              """

