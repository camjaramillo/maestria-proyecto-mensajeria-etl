from sqlalchemy import text
import pandas as pd

from utils.logger import logger

def run_staging(df: pd.DataFrame, session) -> bool:
    """Carga datos a tabla temporal"""
    try:
         # 1. Eliminar tabla temporal si existe (evita problemas con if_exists='replace')
        session.execute(text("DROP TABLE IF EXISTS pg_temp.stg_dim_novedad"))
        session.commit()

        # 2. Crear tabla temporal
        session.execute(text("""
        CREATE TEMPORARY TABLE stg_dim_novedad (
            novedad_id INTEGER NOT NULL,
            nombre VARCHAR(30) NOT NULL
        ) ON COMMIT PRESERVE ROWS;
        """))

        # 3. Cargar datos
        df.to_sql(
            'stg_dim_novedad',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging stg_dim_novedad completado ({len(df)} filas)")
        return True
    
    except Exception as e:
        logger.error(f"Error creando tabla stg_dim_novedad: {str(e)}", exc_info=True)
        return False