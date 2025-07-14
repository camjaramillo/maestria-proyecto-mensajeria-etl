from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple
from utils.database import db_session, DBConnection

def run_extract(start_date: str = None) -> Tuple[pd.DataFrame, bool]:
    """
    Extrae los datos necesarios desde la base de datos de origen y retorna un DataFrame.
    """
    try:
        query = text("""
        SELECT
            servicio_id,
            tipo_novedad_id AS novedad_id,
            mensajero_id,
            descripcion,
            DATE(fecha_novedad) AS fecha_novedad,
            DATE_TRUNC('minute', CAST(fecha_novedad AS time) + INTERVAL '30 seconds')::TIME AS hora_novedad
        FROM
            mensajeria_novedadesservicio
        WHERE 1=1
            AND es_prueba = false
            AND DATE(fecha_novedad) >= :start_date
        ORDER BY 
            servicio_id, fecha_novedad, hora_novedad
        """)

        with db_session(DBConnection.SOURCE) as session:
            df = pd.read_sql(query, session.connection(), params={"start_date": start_date})
            return df, not df.empty

    except Exception as e:
        logger.error("Error extrayendo los datos para fact_novedad_servicio_transaccional", exc_info=True)
        return pd.DataFrame(), False
