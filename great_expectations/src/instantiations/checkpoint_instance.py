from pathlib import Path
import sys
from typing import Protocol, Any

src = Path(__file__).parent.parent
sys.path.append(str(src))
from services.validation import SimpleCheckpoint

class MissingCheckpoint(Exception):
    """Raised when the specified checkpoint does not exist"""

class Checkpoint(Protocol):
    checkpoint_name: str
    yaml_file: str

class CheckpointInstance:
    def __init__(self, type = "simple") -> None:
        self.type = type

    def __get_simple_checkpoint(self, checkpoint_name: str, yaml_file: str):
        return SimpleCheckpoint(checkpoint_name = checkpoint_name, yaml_file = yaml_file)
    
    def get_checkpoint(self, checkpoint_name: str, yaml_file: str) -> Checkpoint:
        if self.type == "simple":
            return self.__get_simple_checkpoint(checkpoint_name, yaml_file)
        else:
            raise MissingCheckpoint()



