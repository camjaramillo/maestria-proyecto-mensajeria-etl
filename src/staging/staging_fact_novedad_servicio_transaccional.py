from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_staging(df: pd.DataFrame, staging_session, target_session=None) -> bool:
    """Carga datos a tabla permanente en STAGING y copia temporal en TARGET"""
    try:
        table_name = 'stg_fact_novedad_servicio_transaccional'

        # 1. Crear tabla permanente en STAGING
        #staging_session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        
        staging_session.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            servicio_id INTEGER NOT NULL,
            novedad_id INTEGER NOT NULL,
            mensajero_id INTEGER,
            descripcion VARCHAR(700),
            fecha_novedad DATE,
            hora_novedad TIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))
        staging_session.commit()

        df.to_sql(
            table_name,
            staging_session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        logger.info(f"Staging STAGING.{table_name} completado ({len(df)} filas)")

        # 2. Crear tabla TEMPORAL en TARGET (si se proporciona sesión)
        if target_session is not None:
            target_session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            target_session.execute(text(f"""
            CREATE TEMPORARY TABLE {table_name} (
                servicio_id INTEGER NOT NULL,
                novedad_id INTEGER NOT NULL,
                mensajero_id INTEGER,
                descripcion VARCHAR(700),
                fecha_novedad DATE,
                hora_novedad TIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ON COMMIT PRESERVE ROWS
            """))
            target_session.commit()

            df.to_sql(
                table_name,
                target_session.connection(),
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Staging TARGET.TEMP {table_name} completado ({len(df)} filas)")

        return True

    except Exception as e:
        logger.error(f"Error en staging {table_name}: {str(e)}", exc_info=True)
        return False
