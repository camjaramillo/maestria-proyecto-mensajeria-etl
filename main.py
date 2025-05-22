# main.py
from src.utils.db import get_engine
from sqlalchemy import text
import sys

def test_connection(env_name: str):
    print(f"Probando conexión a '{env_name}'...")
    try:
        engine = get_engine(env_name)
        # IMPORTANTE: usar text() para las cadenas SQL
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1;"))
            if result.fetchone()[0] == 1:
                print(f"Conexión exitosa a '{env_name}'")
            else:
                print(f"Respuesta inesperada de '{env_name}':", result.fetchone())
    except Exception as e:
        print(f"Error al conectar a '{env_name}': {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_connection("postgres_db_source")
    test_connection("postgres_db_target")
    print("Todas las conexiones funcionan correctamente.")
