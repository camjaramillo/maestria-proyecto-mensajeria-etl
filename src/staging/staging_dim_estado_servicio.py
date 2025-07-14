from sqlalchemy import text
import pandas as pd

from utils.logger import logger

def run_staging(df: pd.DataFrame, staging_session, target_session = None) -> bool:
    """Carga datos a tabla temporal"""
    try:
         # 1. Eliminar tabla temporal si existe (evita problemas con if_exists='replace')
        staging_session.execute(text("DROP TABLE IF EXISTS stg_dim_estado_servicio"))
        staging_session.commit()

        # 2. Crear tabla temporal
        staging_session.execute(text("""
        CREATE TABLE IF NOT EXISTS stg_dim_cliente  (
            estado_servicio_id INTEGER NOT NULL,
            nombre VARCHAR(75) NOT NULL,
            descripcion VARCHAR(500) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP                              
        );
        """))

        # 3. Cargar datos
        df.to_sql(
            'stg_dim_estado_servicio',
            staging_session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging stg_dim_estado_servicio completado ({len(df)} filas)")
        return True
    
    except Exception as e:
        logger.error(f"Error creando tabla stg_dim_estado_servicio: {str(e)}", exc_info=True)
        return False