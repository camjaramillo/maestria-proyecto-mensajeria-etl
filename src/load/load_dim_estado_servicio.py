from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Eliminar tabla
        session.execute(text("DROP TABLE IF EXISTS dim_estado_servicio"))
        session.commit()

        # Crear tabla si no existe
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_estado_servicio (
            estado_servicio_key     INTEGER PRIMARY KEY,
            estado_servicio_id      INTEGER,
            nombre                  VARCHAR(75) NOT NULL,
            descripcion             VARCHAR(500) NOT NULL,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uk_estado_servicio_id UNIQUE (estado_servicio_id)
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            session.execute(text("TRUNCATE TABLE dim_estado_servicio"))
        
        # Cargar datos
        df.to_sql(
            'dim_estado_servicio',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = session.execute(text("SELECT COUNT(*) FROM dim_estado_servicio")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0