from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(staging_session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY novedad_id) AS novedad_key,
            novedad_id,
            UPPER(TRIM(nombre)) AS nombre
        FROM stg_dim_novedad
        """)
        
        df = pd.read_sql(query, staging_session.connection())
        logger.info(f"Transformación completada ({len(df)} filas)")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False