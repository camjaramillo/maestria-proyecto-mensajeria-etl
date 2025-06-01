from typing import Tuple
from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY mensajero_id) AS mensajero_key,
            mensajero_id,
            nombre_usuario,
            UPPER(TRIM(nombre)) AS nombre,
            UPPER(TRIM(apellido)) AS apellido,
            telefono,
            UPPER(TRIM(ciudad_operacion)) AS ciudad_operacion,
            UPPER(TRIM(departamento_operacion)) AS departamento_operacion,
            activo
        FROM pg_temp.stg_dim_mensajero
        """)
        
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación completada ({len(df)} filas)")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False