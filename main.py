# main.py
# Librerías 
import argparse
from datetime import datetime
from pathlib import Path
import sys

# Agregar src/ al path
sys.path.append(str(Path(__file__).resolve().parent / "src")) 

# Scripts
from utils.logger import logger
from pipelines.etl_master import execute as run_etl_master


def main():
    # Fecha actual como string en formato YYYY-MM-DD
    current_date = datetime.today().strftime('%Y-%m-%d')

    parser = argparse.ArgumentParser(description="Ejecuta el ETL con parámetros opcionales")
    parser.add_argument("--etl", help="Nombre del ETL a ejecutar (o 'full')", default="")
    #parser.add_argument('--start_date', type=str, default=current_date, help='Fecha mínima para filtrar datos (YYYY-MM-DD)')
    parser.add_argument('--start_date', type=str, default='2023-01-01', help='Fecha mínima para filtrar datos (YYYY-MM-DD)')
    args = parser.parse_args()
    
    try:
        logger.info(f'***********************************************************************')
        logger.info(f'***********************************************************************')
        # Ejecutar todos los pipelines o seleccionar específicos
        if args.etl.lower() in ("", "full"):
            logger.info("Iniciando ETL Master")
            results = run_etl_master(start_date=args.start_date)
        else:
            logger.info(f"Ejecutando ETL específico: {args.etl}")
            results = run_etl_master(pipelines_to_run=[args.etl], start_date=args.start_date)
        
        # results = run_etl_master(start_date=args.start_date)  # Para todos los pipelines
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
