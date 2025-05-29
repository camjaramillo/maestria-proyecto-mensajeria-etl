# src/utils/database.py
from enum import Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from contextlib import contextmanager
import yaml
from pathlib import Path

class DBConnection(Enum):
    SOURCE = 'postgres_db_src'
    STAGING = 'postgres_db_stg'
    TARGET = 'postgres_db_tgt'

# Cargar la configuración desde config.yaml
def load_db_config():
    config_path = "config/config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)

@contextmanager
def db_session(connection_type):
    """Context manager para sesiones de base de datos"""
    config = load_db_config()[connection_type.value]
    engine = create_engine(
        f"{config['drivername']}://{config['user']}:{config['password']}@"
        f"{config['host']}:{config['port']}/{config['database']}",
        pool_size=config['pool_size'],
        connect_args={"connect_timeout": config['timeout']}
    )
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()

'''
def get_engine(env: DBConnection = DBConnection.SOURCE):
    """
    Devuelve un Engine de SQLAlchemy configurado con:
      - pool_size: tamaño del pool de conexiones
      - connect_timeout: segundos de espera para establecer la conexión
    
    env: valor de la enumeración DBConnection
    """
    cfg = load_config()
    c = cfg[env.value]

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
'''