from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY estado_servicio_id) AS estado_servicio_key,
            estado_servicio_id,
            CASE
                WHEN UPPER(nombre) LIKE '%INICIADO%' THEN 'INICIADO'
                WHEN UPPER(nombre) LIKE '%ASIGNADO%' THEN 'ASIGNADO'
                WHEN UPPER(nombre) LIKE '%NOVEDAD%' THEN 'NOVEDAD'
                WHEN UPPER(nombre) LIKE '%RECOGIDO%' THEN 'RECOGIDO'
                WHEN UPPER(nombre) LIKE '%ENTREGADO%' THEN 'ENTREGADO'
                ELSE 'TERMINADO'
            END AS nombre,
            TRIM(descripcion) AS descripcion       
        FROM pg_temp.stg_dim_estado_servicio
        """)
        
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación completada ({len(df)} filas)")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False