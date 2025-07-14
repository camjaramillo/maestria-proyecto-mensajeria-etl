from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(target_session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging y calcula métricas"""
    try:
        query = text("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY sfnst.servicio_id) AS novedad_transaccional_key,
            fas.servicio_key,
            dn.novedad_key,
            dm.mensajero_key,
            sfnst.descripcion,
            df.fecha_key AS fecha_novedad_key,
            dh.hora_key AS hora_novedad_key
        FROM 
            stg_fact_novedad_servicio_transaccional sfnst
            LEFT JOIN fact_servicio fas ON sfnst.servicio_id = fas.servicio_id
            LEFT JOIN dim_novedad dn ON sfnst.novedad_id = dn.novedad_id
            LEFT JOIN dim_mensajero dm ON sfnst.mensajero_id = dm.mensajero_id
            LEFT JOIN dim_fecha df ON sfnst.fecha_novedad = df.fecha
            LEFT JOIN dim_hora dh ON sfnst.hora_novedad = dh.hora
        WHERE
            fas.servicio_key IS NOT NULL
        """)
        df = pd.read_sql(query, target_session.connection())
        logger.info(f"Transformación completada: {len(df)} filas")
        return df, True
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
