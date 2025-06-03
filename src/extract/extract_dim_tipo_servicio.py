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
            ts.id AS tipo_servicio_id,
            ts.nombre,
            ts.descripcion
        FROM 
            public.mensajeria_tiposervicio ts
        ORDER BY
            tipo_servicio_id
        """

        with db_session(DBConnection.SOURCE) as session:
            return pd.read_sql(query, session.connection()), True
    except Exception as e:
        logger.error("Error extrayendo los datos para dim_tipo_servicio", exc_info=True)
        return pd.DataFrame(), False