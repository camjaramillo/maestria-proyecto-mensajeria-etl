from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_staging(df: pd.DataFrame, staging_session, target_session = None) -> bool:
    """Carga datos a tabla temporal"""
    try:
        # Eliminar tabla temporal si existe
        staging_session.execute(text("DROP TABLE IF EXISTS stg_dim_fecha"))
        staging_session.commit()

        # Crear tabla temporal
        staging_session.execute(text("""
            CREATE TABLE IF NOT EXISTS stg_dim_fecha (
                fecha DATE NOT NULL,
                anio INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                dia INTEGER NOT NULL,
                trimestre INTEGER NOT NULL,
                nombre_mes VARCHAR(20) NOT NULL,
                dia_semana VARCHAR(20) NOT NULL,
                es_festivo BOOLEAN NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP             
            );
        """))

        # Cargar datos
        df.to_sql(
            'stg_dim_fecha',
            staging_session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging stg_dim_fecha completado ({len(df)} filas)")
        return True

    except Exception as e:
        logger.error(f"Error creando tabla stg_dim_fecha: {str(e)}", exc_info=True)
        return False
