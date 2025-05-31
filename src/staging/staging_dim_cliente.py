from sqlalchemy import text
import pandas as pd

from utils.logger import logger

def run_staging(df: pd.DataFrame, session) -> bool:
    """Carga datos a tabla temporal"""
    try:
         # 1. Eliminar tabla temporal si existe (evita problemas con if_exists='replace')
        session.execute(text("DROP TABLE IF EXISTS pg_temp.stg_dim_cliente"))
        session.commit()

        # 2. Crear tabla temporal
        session.execute(text("""
        CREATE TEMPORARY TABLE stg_dim_cliente (
            cliente_id INTEGER NOT NULL,
            nit VARCHAR(50) NOT NULL,
            nombre VARCHAR(120) NOT NULL,
            email VARCHAR(120),
            direccion VARCHAR(250),
            telefono VARCHAR(100),
            ciudad_id INTEGER NOT NULL,
            ciudad VARCHAR(120) NOT NULL,
            departamento_id INTEGER NOT NULL,
            departamento VARCHAR(120) NOT NULL,
            sector VARCHAR(50) NOT NULL,
            activo BOOLEAN NOT NULL
        ) ON COMMIT PRESERVE ROWS;
        """))

        # 3. Cargar datos
        df.to_sql(
            'stg_dim_cliente',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging completado ({len(df)} filas)")
        return True
    
    except Exception as e:
        logger.error("Error creando tabla stg_dim_cliente", exc_info=True)
        return False