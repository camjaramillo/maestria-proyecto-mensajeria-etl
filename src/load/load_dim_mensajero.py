from typing import Tuple
import pandas as pd
from sqlalchemy import text
from utils.logger import logger

def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Crear tabla si no existe
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_mensajero (
            mensajero_key           INT PRIMARY KEY,
            nombre_usuario          VARCHAR(150),
            nombre                  VARCHAR(120),
            apellido                VARCHAR(120),
            telefono                VARCHAR(15),
            ciudad_operacion        VARCHAR(120),
            departamento_operacion  VARCHAR(120),
            activo                  BOOLEAN,
            created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            session.execute(text("TRUNCATE TABLE dim_mensajero"))
        
        # Cargar datos
        df.to_sql(
            'dim_mensajero',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = session.execute(text("SELECT COUNT(*) FROM dim_mensajero")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0