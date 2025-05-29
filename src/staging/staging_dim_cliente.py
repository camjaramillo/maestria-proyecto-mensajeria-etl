from sqlalchemy import text
import pandas as pd

'''
def create_table_stg_dim_cliente():

    Crea la tabla staging para DimCliente.
    Si la tabla ya existe, se elimina antes de crearla nuevamente.
    
    
    engine = get_engine(env="postgres_db_stg")

    drop_stmt = text("DROP TABLE IF EXISTS stg_dim_cliente")

    create_table_sql = text("""
    CREATE TEMPORARY TABLE IF NOT EXISTS stg_dim_cliente (
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
        activo BOOLEAN NOT NULL,
        etl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    with engine.begin() as conn:
        conn.execute(drop_stmt)
        conn.execute(create_table_sql)
        print("Tabla 'stg_dim_cliente' creada.")
'''

def run_staging(df: pd.DataFrame, session):
    """Punto de entrada principal"""
    create_staging_table(session)
    load_to_staging(df, session)


def create_staging_table(session):
    """
    Crea una tabla temporal para stg_dim_cliente.
    Esta tabla se eliminará automáticamente al finalizar la sesión.
    """
    #engine = get_engine(DBConnection.STAGING)

    create_table_sql = text("""
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
    """)

    session.execute(create_table_sql)

    #with engine.begin() as conn:
    #    conn.execute(create_table_sql)
    #    print("Tabla temporal 'stg_dim_cliente' creada.")


def load_to_staging(df: pd.DataFrame, session):
    """
    Carga el DataFrame en la tabla temporal 'stg_dim_cliente'.
    """
    #engine = get_engine(env=DBConnection.STAGING)
    df.to_sql('stg_dim_cliente', con=session.connection(), if_exists='append', index=False)

     # Verificar datos
    count = session.execute(text("SELECT COUNT(*) FROM stg_dim_cliente")).scalar()
    print(f"Tabla temporal 'stg_dim_cliente' creada con {count} registros")

'''
def load_stg_dim_cliente():
    source_engine = get_engine("postgres_db_src")
    staging_engine = get_engine("postgres_db_stg")

    query = """
    SELECT
        c.cliente_id,
        c.nit_cliente AS nit,
        c.nombre,
        c.email,
        c.direccion,
        c.telefono,
        c.ciudad_id,
        ci.nombre AS ciudad,
        d.departamento_id,
        d.nombre AS departamento,
        c.sector,
        c.activo
    FROM public.cliente c
    JOIN public.ciudad ci ON c.ciudad_id = ci.ciudad_id
    JOIN public.departamento d ON ci.departamento_id = d.departamento_id
    """

    df = pd.read_sql(query, source_engine)

    with staging_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg_dim_cliente"))
        df.to_sql("stg_dim_cliente", conn, if_exists="append", index=False)
        print(f"Cargados {len(df)} registros a 'stg_dim_cliente'")
'''