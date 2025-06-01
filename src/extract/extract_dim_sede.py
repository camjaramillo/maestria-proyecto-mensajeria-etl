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
            s.sede_id,
			s.nombre,
			s.direccion,
            s.ciudad_id,
            ci.nombre AS ciudad,
            d.departamento_id,
            d.nombre AS departamento,
			cl.cliente_id,
			cl.nit_cliente,
			cl.nombre AS nombre_cliente
        FROM public.sede s
        JOIN public.ciudad ci ON s.ciudad_id = ci.ciudad_id
        JOIN public.departamento d ON ci.departamento_id = d.departamento_id
		JOIN public.cliente cl ON s.cliente_id = cl.cliente_id
        """

        with db_session(DBConnection.SOURCE) as session:
            return pd.read_sql(query, session.connection()), True
    except Exception as e:
        logger.error("Error extrayendo los datos para dim_sede", exc_info=True)
        return pd.DataFrame(), False

