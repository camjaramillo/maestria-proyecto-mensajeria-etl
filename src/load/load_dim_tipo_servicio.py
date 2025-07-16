from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Eliminar tabla
        #target_session.execute(text("DROP TABLE IF EXISTS dim_tipo_servicio CASCADE"))
        #target_session.commit()

        # Crear tabla si no existe
        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_tipo_servicio (
            tipo_servicio_key       INTEGER PRIMARY KEY,
            tipo_servicio_id        INTEGER,
            nombre                  VARCHAR(75) NOT NULL,
            descripcion             VARCHAR(500) NOT NULL,
            CONSTRAINT uk_tipo_servicio_id UNIQUE (tipo_servicio_id)
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            target_session.execute(text("TRUNCATE TABLE dim_tipo_servicio"))
        
        # Cargar datos
        df.to_sql(
            'dim_tipo_servicio',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = target_session.execute(text("SELECT COUNT(*) FROM dim_tipo_servicio")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0