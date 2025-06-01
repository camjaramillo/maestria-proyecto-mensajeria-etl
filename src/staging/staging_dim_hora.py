from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_staging(df: pd.DataFrame, session) -> bool:
    try:
        session.execute(text("DROP TABLE IF EXISTS pg_temp.stg_dim_hora"))
        session.commit()
        
        session.execute(text("""
            CREATE TEMPORARY TABLE stg_dim_hora (
                hora TIME NOT NULL,
                hora_entera INTEGER NOT NULL,
                minuto INTEGER NOT NULL,
                franja_horaria VARCHAR(20) NOT NULL
            ) ON COMMIT PRESERVE ROWS;
        """))
        
        df.to_sql(
            'stg_dim_hora',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Staging stg_dim_hora completado ({len(df)} filas)")
        return True
    except Exception as e:
        logger.error(f"Error creando tabla stg_dim_hora: {str(e)}", exc_info=True)
        return False
