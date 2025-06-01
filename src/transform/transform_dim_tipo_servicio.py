from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY tipo_servicio_id) AS tipo_servicio_key,
            tipo_servicio_id,
            UPPER(TRIM(nombre)) AS nombre,
            TRIM(descripcion) AS descripcion       
        FROM pg_temp.stg_dim_tipo_servicio
        """)
        
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación completada ({len(df)} filas)")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False