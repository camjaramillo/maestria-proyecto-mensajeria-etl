# src/utils/config.py
import yaml
from pathlib import Path

def load_config(path="config/config.yaml"):
    """Lee y parsea el archivo YAML de configuración."""
    
    with open(path, "r") as file:
        return yaml.safe_load(file)
