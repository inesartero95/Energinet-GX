from typing import Protocol, Any
from instantiations.data_source_instance import DataSource
import great_expectations as gx 
from ruamel import yaml

class CustomExpectationSuite(Protocol):
    expectation_suit_name: str
    suite: Any  

class DataContext:
    
    def __init__(self, datasource: DataSource):
        self.datasource = datasource
    
    @property 
    def context(self):
        context = gx.get_context()
        return context
    
    def save_datasource(self):
        print(f"save datasource: {self.datasource.datasource_name} to context")
        self.context.add_datasource(**yaml.load(self.datasource.yaml_file))
    
    def get_suite(self, expectation_suite_name: str) -> CustomExpectationSuite:
        print(f"suite created called {expectation_suite_name}")
        self.expectation_suite_name = expectation_suite_name
        self.suite = self.context.add_or_update_expectation_suite(expectation_suite_name = self.expectation_suite_name)
        return self.suite
    
    def save_suite(self, suite: Any,  expectation_suite_name: str):
        self.context.save_expectation_suite(expectation_suite = suite, expectation_suite_name = expectation_suite_name)

    def test_checkpoint(self, yaml_file):
        self.checkpoint_config = yaml_file
        self.my_checkpoint = self.context.test_yaml_config(yaml_config = yaml_file)
    
    def add_checkpoint(self):
        self.context.add_checkpoint(**yaml.load(self.checkpoint_config))

    def run_checkpoint(self, checkpoint_name: str):
        self.result = self.context.run_checkpoint(
            checkpoint_name = checkpoint_name,
        )
        print(f'run checkpoint {checkpoint_name} and get validation result')
    
    def save_checkpoint(self, checkpoint_name: str):
        with open(f'{checkpoint_name}_result.json', "w") as outfile:
            outfile.write(str(self.result))

    