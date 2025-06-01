from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    try:
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_hora (
                hora_key         INTEGER PRIMARY KEY,
                hora             TIME NOT NULL,
                hora_entera      INTEGER NOT NULL,
                minuto           INTEGER NOT NULL,
                franja_horaria   VARCHAR(20) NOT NULL,
                created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uk_hora UNIQUE (hora)
            )
        """))
        
        if truncate:
            session.execute(text("TRUNCATE TABLE dim_hora"))
        
        df.to_sql(
            'dim_hora',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        count = session.execute(text("SELECT COUNT(*) FROM dim_hora")).scalar()
        logger.info(f"Carga de dim_hora exitosa. Total registros: {count}")
        return True, len(df)
    except Exception as e:
        logger.error(f"Error en carga de dim_hora: {str(e)}", exc_info=True)
        return False, 0
