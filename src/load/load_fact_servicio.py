from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_load(df: pd.DataFrame, session, truncate: bool = False) -> Tuple[bool, int]:
    """Carga datos a la tabla de hechos final"""
    try:
        # Eliminar tabla
        session.execute(text("DROP TABLE IF EXISTS fact_servicio CASCADE"))
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
            tiempo_total_entrega_min INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uk_fact_servicio_id UNIQUE (servicio_id),
            CONSTRAINT fk_cliente FOREIGN KEY (cliente_key) REFERENCES dim_cliente(cliente_key),
            CONSTRAINT fk_mensajero FOREIGN KEY (mensajero_key) REFERENCES dim_mensajero(mensajero_key),
            CONSTRAINT fk_sede FOREIGN KEY (sede_key) REFERENCES dim_sede(sede_key),
            CONSTRAINT fk_tipo_servicio FOREIGN KEY (tipo_servicio_key) REFERENCES dim_tipo_servicio(tipo_servicio_key),
            CONSTRAINT fk_estado_servicio_final FOREIGN KEY (estado_servicio_final_key) REFERENCES dim_estado_servicio(estado_servicio_key),
            CONSTRAINT fk_fecha_solicitud FOREIGN KEY (fecha_solicitud_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_fecha_iniciado FOREIGN KEY (fecha_iniciado_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_fecha_asignacion FOREIGN KEY (fecha_asignacion_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_fecha_recogida FOREIGN KEY (fecha_recogida_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_fecha_entrega FOREIGN KEY (fecha_entrega_key) REFERENCES dim_fecha(fecha_key),
            CONSTRAINT fk_hora_solicitud FOREIGN KEY (hora_solicitud_key) REFERENCES dim_hora(hora_key),
            CONSTRAINT fk_hora_iniciado FOREIGN KEY (hora_iniciado_key) REFERENCES dim_hora(hora_key),
            CONSTRAINT fk_hora_asignacion FOREIGN KEY (hora_asignacion_key) REFERENCES dim_hora(hora_key),
            CONSTRAINT fk_hora_recogida FOREIGN KEY (hora_recogida_key) REFERENCES dim_hora(hora_key),
            CONSTRAINT fk_hora_entrega FOREIGN KEY (hora_entrega_key) REFERENCES dim_hora(hora_key)
        );
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
