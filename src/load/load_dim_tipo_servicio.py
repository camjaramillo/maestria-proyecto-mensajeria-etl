from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Crear tabla si no existe
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_tipo_servicio (
            tipo_servicio_key       INTEGER PRIMARY KEY,
            tipo_servicio_id        INTEGER,
            nombre                  VARCHAR(75) NOT NULL,
            descripcion             VARCHAR(500) NOT NULL,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uk_tipo_servicio_id UNIQUE (tipo_servicio_id)
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            session.execute(text("TRUNCATE TABLE dim_tipo_servicio"))
        
        # Cargar datos
        df.to_sql(
            'dim_tipo_servicio',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = session.execute(text("SELECT COUNT(*) FROM dim_tipo_servicio")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0