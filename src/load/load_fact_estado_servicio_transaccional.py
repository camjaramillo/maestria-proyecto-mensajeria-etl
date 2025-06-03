from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla de hechos final"""
    try:
        # Eliminar tabla
        session.execute(text("DROP TABLE IF EXISTS fact_estado_servicio_transaccional"))
        session.commit()

        # Crear tabla si no existe
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_estado_servicio_transaccional (
            estado_transaccional_key INTEGER PRIMARY KEY,
            servicio_key INTEGER NOT NULL,
            estado_anterior_key INTEGER,
            estado_nuevo_key INTEGER,
            fecha_cambio_key INTEGER NOT NULL,
            hora_cambio_key INTEGER NOT NULL,
            duracion_estado_minutos INTEGER
        )
        """))
        if truncate:
            session.execute(text("TRUNCATE TABLE fact_estado_servicio_transaccional"))
        df.to_sql(
            'fact_estado_servicio_transaccional',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        count = session.execute(text("SELECT COUNT(*) FROM fact_estado_servicio_transaccional")).scalar()
        logger.info(f"Carga completada: {count} registros en fact_estado_servicio_transaccional")
        return True, len(df)
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0
