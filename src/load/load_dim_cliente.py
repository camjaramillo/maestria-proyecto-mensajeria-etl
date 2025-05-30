from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_load(df: pd.DataFrame, session, truncate: bool = True) -> Tuple[bool, int]:
    """
    Ejecuta todo el proceso de carga
    Retorna: (éxito: bool, registros_cargados: int)
    """
    if df.empty:
        logger.error("DataFrame vacío recibido para carga")
        return False, 0
    
    try:
        success = load_data(df, session, truncate)
        return success, len(df)
    except Exception as e:
        logger.error("Error en execute() de carga", exc_info=True)
        return False, 0
    

def create_dim_table(session) -> bool:
    """Crea la tabla dimensional si no existe"""
    try:
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_cliente (
            cliente_key     INTEGER PRIMARY KEY,
            nit             VARCHAR(20),
            nombre          VARCHAR(120) NOT NULL,
            email           VARCHAR(120),
            direccion       VARCHAR(250),
            telefono        VARCHAR(100),
            ciudad          VARCHAR(120),
            departamento    VARCHAR(120),
            sector          VARCHAR(50),
            activo          BOOLEAN,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))
        return True
    except Exception as e:
        logger.error("Error creando tabla dim_cliente", exc_info=True)
        return False

def truncate_table(session) -> bool:
    """Opcional: Vacía la tabla antes de cargar (si es full refresh)"""
    try:
        session.execute(text("TRUNCATE TABLE dim_cliente"))
        logger.warning("¡Tabla dim_cliente truncada (carga completa)!")
        return True
    except Exception as e:
        logger.error("Error truncando tabla", exc_info=True)
        return False

def load_data(df: pd.DataFrame, session, truncate: bool = False) -> bool:
    """Carga los datos transformados a la tabla dimensional"""
    try:
        # 1. Crear tabla si no existe
        if not create_dim_table(session):
            return False
        
        # 2. Opcional: Vaciar tabla existente
        if truncate and not truncate_table(session):
            return False
        
        # 3. Cargar datos
        df.to_sql(
            'dim_cliente',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',  # Más eficiente para inserts masivos
            chunksize=1000   # Inserta en lotes de 1000 registros
        )
        
        # 4. Verificar carga
        count = session.execute(text("SELECT COUNT(*) FROM dim_cliente")).scalar()
        logger.info(f"Carga exitosa. Total registros en dim_cliente: {count}")
        return True
        
    except Exception as e:
        logger.error("Error en carga de datos", exc_info=True)
        return False