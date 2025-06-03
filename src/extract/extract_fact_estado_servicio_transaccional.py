from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple
from utils.database import db_session, DBConnection

def run_extract() -> Tuple[pd.DataFrame, bool]:
    """
    Extrae los datos necesarios desde la base de datos de origen y retorna un DataFrame.
    """
    try:
        query = text("""
        SELECT
            mes.servicio_id,
            LAG(estado_id) OVER (PARTITION BY mes.servicio_id ORDER BY fecha, hora) AS estado_anterior_id,
            mes.estado_id AS estado_nuevo_id,
            mes.fecha AS fecha_cambio,
            DATE_TRUNC('minute', mes.hora + INTERVAL '30 seconds')::TIME AS hora_cambio
        FROM 
            mensajeria_estadosservicio mes,
            mensajeria_servicio ms
        WHERE 1=1
            AND mes.servicio_id = ms.id
            AND ms.es_prueba = false
        ORDER BY 
            servicio_id, fecha, hora;
        """)

        with db_session(DBConnection.SOURCE) as session:
            return pd.read_sql(query, session.connection()), True

    except Exception as e:
        logger.error("Error extrayendo los datos para fact_estado_servicio_transaccional", exc_info=True)
        return pd.DataFrame(), False
