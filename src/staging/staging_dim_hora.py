from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_staging(df: pd.DataFrame, staging_session, target_session = None) -> bool:
    try:
        staging_session.execute(text("DROP TABLE IF EXISTS stg_dim_hora"))
        staging_session.commit()
        
        staging_session.execute(text("""
            CREATE TABLE IF NOT EXISTS stg_dim_hora (
                hora TIME NOT NULL,
                hora_entera INTEGER NOT NULL,
                minuto INTEGER NOT NULL,
                franja_horaria VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP             
            );
        """))
        
        df.to_sql(
            'stg_dim_hora',
            staging_session.connection(),
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
