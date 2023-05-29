import pandas as pd
from yaml import safe_load
from pathlib import Path
src = Path(__file__).parent.parent

class YamlReadAndConvert:
    def __init__(self, file) -> None:
        self.file = file

    def yaml_convert(self):
      with open(self.file, 'r') as f:
          data = pd.json_normalize(safe_load(f)) 
      return data