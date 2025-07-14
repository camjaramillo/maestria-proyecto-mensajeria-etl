# src/pipelines/etl_base.py

from typing import Callable, Tuple
from utils.database import db_session, DBConnection
from utils.logger import logger


def run_etl_with_staging(
    extract_fn: Callable,
    staging_fn: Callable,
    transform_fn: Callable,
    load_fn: Callable,
    table_name: str,
    truncate: bool = False,
    transform_connection: DBConnection = DBConnection.STAGING,
    start_date: str = None
) -> Tuple[bool, int]:
    """
    Ejecuta un ETL con staging explícito:
    1. Extrae datos (fuente → DataFrame)
    2. Carga a staging (DataFrame → tabla staging)
    3. Transforma (staging/target → DataFrame procesado)
    4. Carga a destino (DataFrame → tabla final)
    """
    try:
        # 1. Extracción
        logger.info(f"Extrayendo datos... {table_name}")
        df_raw, extract_ok = extract_fn()

        if not extract_ok or df_raw.empty:
            logger.error(f"Falló la extracción o DataFrame vacío")
            return False, 0

        with db_session(DBConnection.STAGING) as staging_session, \
             db_session(DBConnection.TARGET) as target_session:

            # 2. Staging
            logger.info(f"Cargando a staging ... {table_name}")
            if not staging_fn(df_raw, staging_session, target_session):
                staging_session.rollback()
                return False, 0

            # 3. Transformación
            logger.info(f"Transformando... {table_name}")

            if transform_connection == DBConnection.STAGING:
                df_transformed, transform_ok = transform_fn(staging_session)
            else:
                df_transformed, transform_ok = transform_fn(target_session)

            if not transform_ok:
                target_session.rollback()
                return False, 0

            # 4. Carga final
            logger.info(f"Cargando a destino... {table_name}")
            load_ok, rows_loaded = load_fn(df_transformed, target_session, truncate)

            if not load_ok:
                target_session.rollback()
                return False, 0

            logger.info(f"ETL completado. Filas cargadas: {rows_loaded} en {table_name}")
            return True, rows_loaded

    except Exception as e:
        logger.error(f"Error en ETL: {str(e)}", exc_info=True)
        return False, 0
