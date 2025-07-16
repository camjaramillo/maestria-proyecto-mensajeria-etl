from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Eliminar tabla
        #target_session.execute(text("DROP TABLE IF EXISTS dim_cliente CASCADE"))
        #target_session.commit()

        # Crear tabla si no existe
        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_cliente (
            cliente_key     INTEGER PRIMARY KEY,
            cliente_id      INTEGER NOT NULL,
            nit             VARCHAR(20),
            nombre          VARCHAR(120) NOT NULL,
            email           VARCHAR(120),
            direccion       VARCHAR(250),
            telefono        VARCHAR(100),
            ciudad          VARCHAR(120),
            departamento    VARCHAR(120),
            sector          VARCHAR(50),
            activo          BOOLEAN,
            CONSTRAINT uk_cliente_id UNIQUE (cliente_id)
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            target_session.execute(text("TRUNCATE TABLE dim_cliente"))
        
        # Cargar datos
        df.to_sql(
            'dim_cliente',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = target_session.execute(text("SELECT COUNT(*) FROM dim_cliente")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0