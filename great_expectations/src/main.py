from pathlib import Path
import sys
import os
import configparser
import logging

src = Path(__file__).parent.parent
sys.path.append(str(src))

from utility.ODBC_connect import ODBC
from services.data_context import DataContext
from services.validation import SimpleCheckpoint
from instantiations.query_store_instance import QueryStoreInstance
from instantiations.data_source_instance import DataSourceInstance
from utility.yaml_reader import YamlReadAndConvert
from factory.expectation_factory import factory

class MissingExpectationType(Exception):
    """Raised when the expecation type of an expectation class is not valid. 
    Should be either 'data', 'static' or 'custom'"""

# initialize 
script_path = os.path.realpath(os.path.dirname(__file__))
os.chdir(script_path)
config = configparser.ConfigParser()
config.read('..\..\secrets.ini') #TODO: Change to use pathlib
server = config['Server info']

# Add connection
odbc = ODBC(server, "ActiveDirectoryPassword")

# Get information from data owners #TODO: Change to use pathlib
# yaml = YamlReadAndConvert("data_owner_assumptions\\data_owner.yml")
# yaml = YamlReadAndConvert("data_owner_assumptions\\scada_el__eta_user30_e_ana_1h.yml") 
# yaml = YamlReadAndConvert("data_owner_assumptions\\scada_el__eta_user30_e_ana_1m.yml")
# yaml = YamlReadAndConvert("data_owner_assumptions\\shared__calendar.yml")
# yaml = YamlReadAndConvert("data_owner_assumptions\\svk__eds_fcr.yml")
yaml = YamlReadAndConvert("data_owner_assumptions\\regelleistung__fcr.yml")
data = yaml.yaml_convert()

asset_name = f"{data['schema'].values[0]}.{data['name'].values[0]}" 
data_source_name = f"{data['schema'].values[0]}__{data['name'].values[0]}_datasource"
my_suite_name = f"{data['schema'].values[0]}__{data['name'].values[0]}_suite"

# Create DataSource and Context
data_source = DataSourceInstance().get_datasource(datasource_name = data_source_name, connection_str = odbc.url_conn_str)

data_context = DataContext(data_source)

data_context.context
data_context.save_datasource()

# Create a new suite
my_suite = data_context.get_suite(my_suite_name)
data_context.save_suite(my_suite, my_suite_name)

# Add expectations to set suite
for l in data.columns:
    if "Expectations" in l:
        for value in data[l].values:
            name_of_expectation = (data[l].name).partition(".")[2]
            for item in value:
                  expectation_config = factory(name_of_expectation)

                  if expectation_config.type == 'static':
                      config_initiate = expectation_config(item).set_expectation_config()
                  elif expectation_config.type == 'data':
                      query_store = QueryStoreInstance().get_store(odbc._conn_str,asset_name)
                      config_initiate = expectation_config(item, query_store).set_expectation_config()
                  elif expectation_config.type == 'custom':
                      config_initiate = expectation_config(item, asset_name).set_expectation_config()
                  else:
                      raise MissingExpectationType()

                  my_suite.add_expectation(config_initiate)
                  data_context.context.save_expectation_suite(my_suite)


# Add checkpoint -----------------------------------------
checkpoint_name = f"{data['schema'].values[0]}__{data['name'].values[0]}_checkpoint"

checkpoint_config = SimpleCheckpoint(checkpoint_name, my_suite_name, asset_name, data_source_name).checkpoint_config

data_context.test_checkpoint(checkpoint_config)

data_context.add_checkpoint()

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

data_context.run_checkpoint(checkpoint_name)

data_context.save_checkpoint(checkpoint_name = f"{checkpoint_name}_test")

data_context.context.open_data_docs()
