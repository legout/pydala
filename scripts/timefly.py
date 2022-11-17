import typer
import rtoml
from ..src.pydala.utils.base import NestedDictReplacer

app = typer.Typer()


def load_config(path:str):
    with open(path) as f:
        return NestedDictReplacer(rtoml.load(f)).replace("None", None)

    
def init(config_path:str):
    config = load_config(path=config_path)

    
def add_snapshot(config_path:str):
    config = load_config(path=config_path)


def delete_snapshot(config_path:str):
    config = load_config(path=config_path)




    
