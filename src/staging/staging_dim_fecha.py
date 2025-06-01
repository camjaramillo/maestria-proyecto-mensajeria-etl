from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_staging(df: pd.DataFrame, session) -> bool:
    """Carga datos a tabla temporal"""
    try:
        # Eliminar tabla temporal si existe
        session.execute(text("DROP TABLE IF EXISTS pg_temp.stg_dim_fecha"))
        session.commit()

        # Crear tabla temporal
        session.execute(text("""
            CREATE TEMPORARY TABLE stg_dim_fecha (
                fecha DATE NOT NULL,
                anio INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                dia INTEGER NOT NULL,
                trimestre INTEGER NOT NULL,
                nombre_mes VARCHAR(20) NOT NULL,
                dia_semana VARCHAR(20) NOT NULL,
                es_festivo BOOLEAN NOT NULL
            ) ON COMMIT PRESERVE ROWS;
        """))

        # Cargar datos
        df.to_sql(
            'stg_dim_fecha',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging stg_dim_fecha completado ({len(df)} filas)")
        return True

    except Exception as e:
        logger.error(f"Error creando tabla stg_dim_fecha: {str(e)}", exc_info=True)
        return False
