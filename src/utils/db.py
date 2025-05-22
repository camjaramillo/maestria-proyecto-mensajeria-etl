# src/utils/db.py
import yaml
from sqlalchemy import create_engine

def load_config(path="config/config.yaml"):
    """Lee y parsea el archivo YAML de configuración."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def get_engine(env="postgres_db_source"):
    """
    Devuelve un Engine de SQLAlchemy configurado con:
      - pool_size: tamaño del pool de conexiones
      - connect_timeout: segundos de espera para establecer la conexión
    
    env: clave en el YAML ('postgres_db_source' o 'postgres_db_target')
    """
    cfg = load_config()
    c = cfg[env]

    # 1. Construye la URL de conexión en formato SQLAlchemy
    url = (
        f"{c['drivername']}://{c['user']}:{c['password']}@"
        f"{c['host']}:{c['port']}/{c['database']}"
    )

    # 2. Llama a create_engine() pasando pool_size y connect_args
    return create_engine(
        url,
        pool_size=c.get("pool_size", 5),                            # :contentReference[oaicite:0]{index=0}
        connect_args={"connect_timeout": c.get("timeout", 30)}      # :contentReference[oaicite:1]{index=1}
    )
