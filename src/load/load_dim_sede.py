from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Eliminar tabla
        #target_session.execute(text("DROP TABLE IF EXISTS dim_sede CASCADE"))
        #target_session.commit()

        # Crear tabla si no existe
        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_sede (
            sede_key          INTEGER PRIMARY KEY,
            sede_id           INTEGER NOT NULL,
            nombre            VARCHAR(120) NOT NULL,
            direccion         VARCHAR(250),
            ciudad            VARCHAR(120) NOT NULL,
            departamento      VARCHAR(120) NOT NULL,
            cliente_id        INTEGER NOT NULL,                 
            nit_cliente       VARCHAR(50) NOT NULL,
            nombre_cliente    VARCHAR(120) NOT NULL,
            CONSTRAINT uk_sede_id UNIQUE (sede_id)
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            target_session.execute(text("TRUNCATE TABLE dim_sede"))
        
        # Cargar datos
        df.to_sql(
            'dim_sede',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = target_session.execute(text("SELECT COUNT(*) FROM dim_sede")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0