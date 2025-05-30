from sqlalchemy import text
import pandas as pd

from utils import logger

def run_staging(df: pd.DataFrame, session):
    """Punto de entrada principal"""
    if create_staging_table(session):
        return load_to_staging(df, session)

    return False

def create_staging_table(session) -> bool:
    """
    Crea una tabla temporal para stg_dim_cliente.
    Esta tabla se eliminará automáticamente al finalizar la sesión.
    """
    try:
        session.execute(text("""
        CREATE TEMPORARY TABLE stg_dim_cliente (
            cliente_id INTEGER NOT NULL,
            nit VARCHAR(50) NOT NULL,
            nombre VARCHAR(120) NOT NULL,
            email VARCHAR(120),
            direccion VARCHAR(250),
            telefono VARCHAR(100),
            ciudad_id INTEGER NOT NULL,
            ciudad VARCHAR(120) NOT NULL,
            departamento_id INTEGER NOT NULL,
            departamento VARCHAR(120) NOT NULL,
            sector VARCHAR(50) NOT NULL,
            activo BOOLEAN NOT NULL
        ) ON COMMIT PRESERVE ROWS;
        """))
        return True
    except Exception as e:
        logger.error("Error creando tabla stg_dim_cliente", exc_info=True)
        return False


def load_to_staging(df: pd.DataFrame, session) -> bool:
    """
    Carga el DataFrame en la tabla temporal 'stg_dim_cliente'.
    """
    try:
        df.to_sql('stg_dim_cliente', con=session.connection(), if_exists='append', index=False)
        # Verificar datos
        count = session.execute(text("SELECT COUNT(*) FROM stg_dim_cliente")).scalar()
        print(f"Tabla temporal 'stg_dim_cliente' creada con {count} registros")

        return True
    except Exception as e:
        logger.error("Error cargando los datos a tabla stg_dim_cliente", exc_info=True)
        return False
     