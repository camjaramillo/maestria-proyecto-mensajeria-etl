from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla de hechos final"""
    try:
        # Eliminar tabla
        session.execute(text("DROP TABLE IF EXISTS fact_servicio"))
        session.commit()

        # Crear tabla si no existe
        session.execute(text("""
        CREATE TABLE IF NOT EXISTS fact_servicio (
            servicio_key INTEGER PRIMARY KEY,
            servicio_id INTEGER,
            cliente_key INTEGER,
            mensajero_key INTEGER,
            sede_key INTEGER,
            tipo_servicio_key INTEGER,
            estado_servicio_final_key INTEGER,
            prioridad VARCHAR(50),
            ciudad_origen VARCHAR(50),
            departamento_origen VARCHAR(50),
            ciudad_destino VARCHAR(50),
            departamento_destino VARCHAR(50),              
            fecha_solicitud_key INTEGER,
            hora_solicitud_key INTEGER,
            fecha_iniciado_key INTEGER,
            hora_iniciado_key INTEGER,
            fecha_asignacion_key INTEGER,
            hora_asignacion_key INTEGER,
            fecha_recogida_key INTEGER,
            hora_recogida_key INTEGER,
            fecha_entrega_key INTEGER,
            hora_entrega_key INTEGER,
            fecha_cierre_key INTEGER,
            hora_cierre_key INTEGER,
            tiempo_total_entrega INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uk_fact_servicio_id UNIQUE (servicio_id)
        )
        """))
        if truncate:
            session.execute(text("TRUNCATE TABLE fact_servicio"))
        df.to_sql(
            'fact_servicio',
            session.connection(),
            if_exists='append',
            index=False,
            chunksize=1000
        )
        count = session.execute(text("SELECT COUNT(*) FROM fact_servicio")).scalar()
        logger.info(f"Carga completada: {count} registros en fact_servicio")
        return True, len(df)
    except Exception as e:
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        return False, 0
