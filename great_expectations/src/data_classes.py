from dataclasses import dataclass

@dataclass
class DataSource:
    name: str
    connection_string: str
    yaml_file: str