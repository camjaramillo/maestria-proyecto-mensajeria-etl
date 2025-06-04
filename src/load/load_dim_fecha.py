from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla dimensional final"""
    try:
        # Eliminar tabla
        session.execute(text("DROP TABLE IF EXISTS dim_fecha CASCADE"))
        session.commit()

        # Crear tabla si no existe
        session.execute(text("""
            CREATE TABLE IF NOT EXISTS dim_fecha (
                fecha_key     INTEGER PRIMARY KEY,
                fecha         DATE NOT NULL,
                anio          INTEGER NOT NULL,
                mes           INTEGER NOT NULL,
                dia           INTEGER NOT NULL,
                trimestre     INTEGER NOT NULL,
                nombre_mes    VARCHAR(20) NOT NULL,
                dia_semana    VARCHAR(20) NOT NULL,
                es_festivo    BOOLEAN NOT NULL,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uk_fecha UNIQUE (fecha)
            )
        """))

        # Vaciar tabla si es full refresh
        if truncate:
            session.execute(text("TRUNCATE TABLE dim_fecha"))

        # Cargar datos
        df.to_sql(
            'dim_fecha',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )

        # Verificar conteo
        count = session.execute(text("SELECT COUNT(*) FROM dim_fecha")).scalar()
        logger.info(f"Carga exitosa. Total registros: {count}")
        return True, len(df)

    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0
