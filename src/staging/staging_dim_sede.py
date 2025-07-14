from sqlalchemy import text
import pandas as pd

from utils.logger import logger

def run_staging(df: pd.DataFrame, staging_session, target_session = None) -> bool:
    """Carga datos a tabla temporal"""
    try:
         # 1. Eliminar tabla temporal si existe (evita problemas con if_exists='replace')
        staging_session.execute(text("DROP TABLE IF EXISTS stg_dim_sede"))
        staging_session.commit()

        # 2. Crear tabla temporal
        staging_session.execute(text("""
        CREATE TABLE IF NOT EXISTS stg_dim_sede (
            sede_id INTEGER NOT NULL,
            nombre VARCHAR(120) NOT NULL,
            direccion VARCHAR(250),
            ciudad_id INTEGER NOT NULL,
            ciudad VARCHAR(120) NOT NULL,
            departamento_id INTEGER NOT NULL,
            departamento VARCHAR(120) NOT NULL,
            cliente_id INTEGER NOT NULL,                 
            nit_cliente VARCHAR(50) NOT NULL,
            nombre_cliente VARCHAR(120) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP                  
        );
        """))

        # 3. Cargar datos
        df.to_sql(
            'stg_dim_sede',
            staging_session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging stg_dim_sede completado ({len(df)} filas)")
        return True
    
    except Exception as e:
        logger.error(f"Error creando tabla stg_dim_sede: {str(e)}", exc_info=True)
        return False