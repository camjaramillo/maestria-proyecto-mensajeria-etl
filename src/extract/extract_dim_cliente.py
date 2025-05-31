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
            c.cliente_id,
            c.nit_cliente AS nit,
            c.nombre,
            c.email,
            c.direccion,
            c.telefono,
            c.ciudad_id,
            ci.nombre AS ciudad,
            d.departamento_id,
            d.nombre AS departamento,
            c.sector,
            c.activo
        FROM public.cliente c
        JOIN public.ciudad ci ON c.ciudad_id = ci.ciudad_id
        JOIN public.departamento d ON ci.departamento_id = d.departamento_id
        """

        with db_session(DBConnection.SOURCE) as session:
            return pd.read_sql(query, session.connection()), True
    except Exception as e:
        logger.error("Error extrayendo los datos para dim_cliente", exc_info=True)
        return pd.DataFrame(), False

