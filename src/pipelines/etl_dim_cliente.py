"""
Pipeline ETL para DimCliente

Este script orquesta todo el flujo ETL (Extract-Transform-Load) para la dimensión DimCliente.

Pasos:
1. Crea la tabla de staging (si no existe).
2. Extrae los datos de la base fuente, los transforma y los carga en la tabla de staging.
3. Realiza la transformación adicional y carga a la tabla final en el Data Warehouse.

Este pipeline está diseñado para ser ejecutado desde el entrypoint (main.py), o de forma independiente.
"""


from extract.extract_dim_cliente import extract_data
from staging.staging_dim_cliente import run_staging
from utils.database import db_session, DBConnection
from utils.logger import logger 

import time

def run_etl_pipeline():
    """Orquesta todo el proceso ETL"""
    try:
        # 1. Extracción
        logger.info("Iniciando extracción...")
        df_raw = extract_data()
        
        # 2-4. Staging → Transform → Load en una sola sesión
        with db_session(DBConnection.TARGET) as session:

            logger.info("Cargando datos a staging...")
            run_staging(df_raw, session)
    
            #logger.info("Transformando datos...")
            #df_transformed = transform_dim_cliente.execute(session)
            
            #logger.info("Cargando a DW...")
            #load_dim_cliente.execute(df_transformed, session)
            
            # Limpieza opcional
            #session.execute("DROP TABLE IF EXISTS stg_dim_cliente")
            
        logger.info("Proceso completado exitosamente!")
    
    except Exception as e:
        logger.error(f"Error en el pipeline: {str(e)}")
        raise
    
    """
    Ejecuta el ETL para DimCliente.
    """
    #print("Iniciando pipeline ETL para DimCliente...")

    # Paso 1: Extracción
    #df_raw = extract_data()

    # Paso 2: Staging (recibe los datos de la extracción)
    #run_staging(df_raw)

    # Paso 3: [En desarrollo] Transformación y carga al DW
    #print("ETL DimCliente completo (hasta staging).")
