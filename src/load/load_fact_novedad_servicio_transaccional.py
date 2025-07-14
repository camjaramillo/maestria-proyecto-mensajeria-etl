from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, target_session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla de hechos final"""
    try:
        # Eliminar tabla
        target_session.execute(text("DROP TABLE IF EXISTS fact_novedad_servicio_transaccional CASCADE"))
        target_session.commit()

        # Crear tabla si no existe
        target_session.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_novedad_servicio_transaccional (
            novedad_transaccional_key INTEGER PRIMARY KEY,
            servicio_key INTEGER NOT NULL,
            novedad_key INTEGER NOT NULL,
            mensajero_key INTEGER,
            descripcion VARCHAR(700),
            fecha_novedad_key INTEGER NOT NULL,
            hora_novedad_key INTEGER NOT NULL,
            CONSTRAINT fk_servicio FOREIGN KEY (servicio_key) REFERENCES fact_servicio(servicio_key),
            CONSTRAINT fk_novedad FOREIGN KEY (novedad_key) REFERENCES dim_novedad(novedad_key),
            CONSTRAINT fk_mensajero FOREIGN KEY (mensajero_key) REFERENCES dim_mensajero(mensajero_key),
            CONSTRAINT fk_fecha_novedad FOREIGN KEY (fecha_novedad_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_hora_novedad FOREIGN KEY (hora_novedad_key) REFERENCES dim_hora(hora_key)
        );
        """))
        if truncate:
            target_session.execute(text("TRUNCATE TABLE fact_novedad_servicio_transaccional"))
        df.to_sql(
            'fact_novedad_servicio_transaccional',
            target_session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        count = target_session.execute(text("SELECT COUNT(*) FROM fact_novedad_servicio_transaccional")).scalar()
        logger.info(f"Carga completada: {count} registros en fact_novedad_servicio_transaccional")
        return True, len(df)
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0
