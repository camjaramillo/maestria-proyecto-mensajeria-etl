from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def validate_staging_table(session) -> bool:
    """Verifica que la tabla temporal existe y tiene datos"""
    try:
        # 1. Verificar existencia (forma correcta en PostgreSQL)
        exists = session.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema LIKE 'pg_temp%'
                AND table_name = 'stg_dim_cliente'
            )
        """)).scalar()
        
        if not exists:
            logger.error("La tabla stg_dim_cliente no existe en el esquema temporal")
            return False
        
        # 2. Verificar filas
        count = session.execute(text("SELECT COUNT(*) FROM pg_temp.stg_dim_cliente")).scalar()
        
        if count == 0:
            logger.warning("La tabla stg_dim_cliente está vacía")
            return False
            
        return True
    
    except Exception as e:
        logger.error("Error validando staging table", exc_info=True)
        return False

def transform_data(session) -> pd.DataFrame:
    """Transforma datos desde la tabla temporal"""
    try:
        query = text("""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY cliente_id) AS cliente_key, -- SKU secuencial
            nit,
            UPPER(TRIM(nombre)) AS nombre,
            email,
            direccion,
            telefono,
            UPPER(TRIM(ciudad)) AS ciudad,
            UPPER(TRIM(departamento)) AS departamento,
            UPPER(TRIM(sector)) AS sector,
            activo
        FROM pg_temp.stg_dim_cliente
        """)
        
        return pd.read_sql(query, session.connection())
        
    except Exception as e:
        logger.error("Error transformando datos", exc_info=True)
        raise

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """
    Ejecutar todo el proceso de transformación
    Retorna: (DataFrame transformado, éxito booleano)
    """
    if not validate_staging_table(session):
        return pd.DataFrame(), False
    
    try:
        df = transform_data(session)
        logger.info(f"Transformación exitosa. Filas procesadas: {len(df)}")
        return df, True
    except Exception as e:
        return pd.DataFrame(), False