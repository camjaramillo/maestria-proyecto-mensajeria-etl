from typing import Tuple
import pandas as pd
from utils.database import db_session, DBConnection
from utils.logger import logger

def run_extract() -> Tuple[pd.DataFrame, bool]:
    """Extrae datos de mensajeros y sus relaciones"""
    try:
        query = """
        SELECT 
            m.id AS mensajero_id,
            u.username AS nombre_usuario,
            u.first_name AS nombre,
            u.last_name AS apellido,
            m.telefono,
            c.nombre AS ciudad_operacion,
            d.nombre AS departamento_operacion,
            m.activo
        FROM 
            clientes_mensajeroaquitoy m
            JOIN auth_user u ON m.user_id = u.id
            LEFT JOIN ciudad c ON m.ciudad_operacion_id = c.ciudad_id
            LEFT JOIN departamento d ON c.departamento_id = d.departamento_id
        ORDER BY
            mensajero_id
        """
        
        with db_session(DBConnection.SOURCE) as session:
            df = pd.read_sql(query, session.connection())
        
        logger.info(f"Extraídos {len(df)} registros de mensajeros")
        return df, True
        
    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}", exc_info=True)
        return pd.DataFrame(), False