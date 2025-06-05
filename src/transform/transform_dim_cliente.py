from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple


def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY cliente_id) AS cliente_key,
            cliente_id,        
            nit,
            UPPER(TRIM(nombre)) AS nombre,
            email,
            direccion,
            telefono,
            UPPER(TRIM(ciudad)) AS ciudad,
            UPPER(TRIM(departamento)) AS departamento,
            UPPER(TRIM(sector)) AS sector,
            activo
        FROM pg_temp.stg_dim_cliente
        """)
        
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación completada ({len(df)} filas)")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False