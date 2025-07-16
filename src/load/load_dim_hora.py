from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    try:
        # Eliminar tabla
        #target_session.execute(text("DROP TABLE IF EXISTS dim_hora CASCADE"))
        #target_session.commit()

        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS dim_hora (
            hora_key         INTEGER PRIMARY KEY,
            hora             TIME NOT NULL,
            hora_entera      INTEGER NOT NULL,
            minuto           INTEGER NOT NULL,
            franja_horaria   VARCHAR(20) NOT NULL,
            CONSTRAINT uk_hora UNIQUE (hora)
        )
        """))
        
        if truncate:
            target_session.execute(text("TRUNCATE TABLE dim_hora"))
        
        df.to_sql(
            'dim_hora',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        count = target_session.execute(text("SELECT COUNT(*) FROM dim_hora")).scalar()
        logger.info(f"Carga de dim_hora exitosa. Total registros: {count}")
        return True, len(df)
    except Exception as e:
        logger.error(f"Error en carga de dim_hora: {str(e)}", exc_info=True)
        return False, 0
