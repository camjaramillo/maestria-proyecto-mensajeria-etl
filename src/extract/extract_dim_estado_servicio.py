from typing import Tuple
import pandas as pd
from utils.logger import logger
from utils.database import db_session, DBConnection

def run_extract() -> Tuple[pd.DataFrame, bool]:
    """
    Extrae los datos necesarios desde la base de datos de origen y retorna un DataFrame.
    """
    try:
        query = """
        SELECT
            es.id AS estado_servicio_id,
            es.nombre,
            es.descripcion
        FROM 
            public.mensajeria_estado es
        ORDER BY
            estado_servicio_id
        """

        with db_session(DBConnection.SOURCE) as session:
            return pd.read_sql(query, session.connection()), True
    except Exception as e:
        logger.error("Error extrayendo los datos para dim_estado_servicio", exc_info=True)
        return pd.DataFrame(), False

