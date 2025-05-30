# main.py
# Librerías 
from pathlib import Path
import sys

# Agregar src/ al path
sys.path.append(str(Path(__file__).resolve().parent / "src")) 

# Scripts
from utils.logger import logger
from pipelines.etl_master import execute as run_etl_master

'''
def test_connection(env_name: str):
    print(f"Probando conexión a '{env_name}'...")
    try:
        engine = get_engine(env_name)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1;"))
            if result.fetchone()[0] == 1:
                print(f"Conexión exitosa a '{env_name}'")
            else:
                print(f"Respuesta inesperada de '{env_name}':", result.fetchone())
    except Exception as e:
        print(f"Error al conectar a '{env_name}': {e}")
        sys.exit(1)
'''

def main():
    try:
        logger.info("Iniciando ETL Master")
        
        # Ejecutar todos los pipelines o seleccionar específicos
        results = run_etl_master()  # Para todos los pipelines
        # results = run_etl_master(['dim_cliente'])  # Solo DimCliente
        
        # Mostrar resumen
        logger.info("Resumen de ejecución:")

        for pipeline, success in results.items():
            status = "ÉXITO" if success else "FALLÓ"
            logger.info(f"  - {pipeline.upper():<15}: {status}")
            
        if all(results.values()):
            logger.info("¡Todos los pipelines completados con éxito!")
        else:
            logger.error("Algunos pipelines fallaron. Ver logs.")
            
    except Exception as e:
        logger.error(f"Error crítico en main: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
