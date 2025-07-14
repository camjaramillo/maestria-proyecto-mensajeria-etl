from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(staging_session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY sede_id) AS sede_key,
            sede_id,
            UPPER(TRIM(nombre)) AS nombre,
            direccion,
            UPPER(TRIM(ciudad)) AS ciudad,
            UPPER(TRIM(departamento)) AS departamento,
            cliente_id,                 
            nit_cliente,
            UPPER(TRIM(nombre_cliente)) AS nombre_cliente
        FROM stg_dim_sede
        """)
        
        df = pd.read_sql(query, staging_session.connection())
        logger.info(f"Transformación completada ({len(df)} filas)")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False