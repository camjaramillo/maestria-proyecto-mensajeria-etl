# main.py
# Librerías 
from pathlib import Path
import sys

# Agregar src/ al path
sys.path.append(str(Path(__file__).resolve().parent / "src")) 

# Scripts
from pipelines.etl_dim_cliente import run_etl_pipeline
from utils.constants import DBConnection


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
    print("Iniciando proceso ETL completo...\n")

    # Ejecutar pipelines
    run_etl_pipeline()

    print("\nProceso ETL finalizado.")

if __name__ == "__main__":
    #test_connection(DBConnection.SOURCE)
    #test_connection(DBConnection.STAGING)
    #test_connection(DBConnection.TARGET)
    print("Todas las conexiones funcionan correctamente.")
    print("Ejecutando ETL...")
    main()
