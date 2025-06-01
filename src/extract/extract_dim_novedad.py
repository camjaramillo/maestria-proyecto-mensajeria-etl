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
            tn.id AS novedad_id,
            tn.nombre
        FROM public.mensajeria_tiponovedad tn
        """

        with db_session(DBConnection.SOURCE) as session:
            return pd.read_sql(query, session.connection()), True
    except Exception as e:
        logger.error("Error extrayendo los datos para dim_novedad", exc_info=True)
        return pd.DataFrame(), False

