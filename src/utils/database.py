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