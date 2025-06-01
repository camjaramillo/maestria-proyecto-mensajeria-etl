from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    try:
        query = text("""
            SELECT 
                ROW_NUMBER() OVER (ORDER BY hora ASC) AS hora_key,
                hora,
                hora_entera,
                minuto,
                franja_horaria
            FROM pg_temp.stg_dim_hora
        """)
        
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación de dim_hora completada ({len(df)} filas)")
        return df, True
    except Exception as e:
        logger.error(f"Error en transformación de dim_hora: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
