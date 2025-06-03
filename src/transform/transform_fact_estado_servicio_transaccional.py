from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging y calcula métricas"""
    try:
        query = text("""
        SELECT
            ROW_NUMBER() OVER (ORDER BY sfest.servicio_id) AS estado_transaccional_key,
            fas.servicio_key,
            desa.estado_servicio_key AS estado_anterior_key,
            desn.estado_servicio_key AS estado_nuevo_key,
            df.fecha_key AS fecha_cambio_key,
            dh.hora_key AS hora_cambio_key,
            ROUND(
                EXTRACT(EPOCH FROM (
                (sfest.fecha_cambio + sfest.hora_cambio) - 
                LAG(sfest.fecha_cambio + sfest.hora_cambio) 
                    OVER (PARTITION BY sfest.servicio_id ORDER BY sfest.fecha_cambio, sfest.hora_cambio)
                )) / 60
            ) AS duracion_estado_minutos
        FROM
            stg_fact_estado_servicio_transaccional sfest
            LEFT JOIN fact_servicio fas ON sfest.servicio_id = fas.servicio_id
            LEFT JOIN dim_estado_servicio desa ON sfest.estado_anterior_id = desa.estado_servicio_id
            LEFT JOIN dim_estado_servicio desn ON sfest.estado_nuevo_id = desn.estado_servicio_id
            LEFT JOIN dim_fecha df ON sfest.fecha_cambio = df.fecha
            LEFT JOIN dim_hora dh ON sfest.hora_cambio = dh.hora
        """)
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación completada: {len(df)} filas")
        return df, True
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
