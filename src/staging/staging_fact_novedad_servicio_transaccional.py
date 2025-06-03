from sqlalchemy import text
import pandas as pd

from utils.logger import logger

def run_staging(df: pd.DataFrame, session) -> bool:
    """Carga datos a tabla temporal de staging"""
    try:

        # 1. Eliminar tabla temporal si existe (evita problemas con if_exists='replace')
        session.execute(text("DROP TABLE IF EXISTS pg_temp.stg_fact_novedad_servicio_transaccional"))
        session.commit()

        # 2. Crear tabla temporal
        session.execute(text("""
        CREATE TEMPORARY TABLE stg_fact_novedad_servicio_transaccional (
            servicio_id INTEGER NOT NULL,
            novedad_id INTEGER NOT NULL,
            mensajero_id INTEGER,
            descripcion VARCHAR(700),
            fecha_novedad DATE,
            hora_novedad TIME
        ) ON COMMIT PRESERVE ROWS;
        """))

        # 3. Cargar datos
        df.to_sql(
            'stg_fact_novedad_servicio_transaccional',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging fact_novedad_servicio_transaccional completado ({len(df)} filas)")
        return True
    
    except Exception as e:
        logger.error(f"Error creando tabla fact_novedad_servicio_transaccional: {str(e)}", exc_info=True)
        return False
