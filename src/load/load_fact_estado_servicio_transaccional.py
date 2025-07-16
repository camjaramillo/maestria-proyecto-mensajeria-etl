from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla de hechos final"""
    try:
        # Eliminar tabla
        #target_session.execute(text("DROP TABLE IF EXISTS fact_estado_servicio_transaccional CASCADE"))
        #target_session.commit()

        # Crear tabla si no existe
        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_estado_servicio_transaccional (
            estado_transaccional_key INTEGER PRIMARY KEY,
            servicio_key INTEGER NOT NULL,
            estado_anterior_key INTEGER,
            estado_nuevo_key INTEGER,
            fecha_cambio_key INTEGER NOT NULL,
            hora_cambio_key INTEGER NOT NULL,
            duracion_estado_minutos INTEGER,
            CONSTRAINT fk_servicio FOREIGN KEY (servicio_key) REFERENCES fact_servicio(servicio_key),
            CONSTRAINT fk_estado_anterior FOREIGN KEY (estado_anterior_key) REFERENCES dim_estado_servicio(estado_servicio_key),
            CONSTRAINT fk_estado_nuevo FOREIGN KEY (estado_nuevo_key) REFERENCES dim_estado_servicio(estado_servicio_key),
            CONSTRAINT fk_fecha_cambio FOREIGN KEY (fecha_cambio_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_hora_cambio FOREIGN KEY (hora_cambio_key) REFERENCES dim_hora(hora_key)
        );
        """))
        if truncate:
            target_session.execute(text("TRUNCATE TABLE fact_estado_servicio_transaccional CASCADE"))
        df.to_sql(
            'fact_estado_servicio_transaccional',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        count = target_session.execute(text("SELECT COUNT(*) FROM fact_estado_servicio_transaccional")).scalar()
        logger.info(f"Carga completada: {count} registros en fact_estado_servicio_transaccional")
        return True, len(df)
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0
