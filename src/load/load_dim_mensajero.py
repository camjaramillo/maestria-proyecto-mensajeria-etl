from typing import Tuple
import pandas as pd
from sqlalchemy import text
from utils.logger import logger

def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Eliminar tabla
        #target_session.execute(text("DROP TABLE IF EXISTS dim_mensajero CASCADE"))
        #target_session.commit()

        # Crear tabla si no existe
        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_mensajero (
            mensajero_key           INT PRIMARY KEY,
            mensajero_id            INT,
            nombre_usuario          VARCHAR(150),
            nombre                  VARCHAR(120),
            apellido                VARCHAR(120),
            telefono                VARCHAR(15),
            ciudad_operacion        VARCHAR(120),
            departamento_operacion  VARCHAR(120),
            activo                  BOOLEAN,
            CONSTRAINT uk_mensajero_id UNIQUE (mensajero_id)
        )
        """))
        
        # Vaciar tabla si es full refresh
        if truncate:
            target_session.execute(text("TRUNCATE TABLE dim_mensajero"))
        
        # Cargar datos
        df.to_sql(
            'dim_mensajero',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Verificar conteo
        count = target_session.execute(text("SELECT COUNT(*) FROM dim_mensajero")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)
        
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0