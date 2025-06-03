from sqlalchemy import text
import pandas as pd

from utils.logger import logger

def run_staging(df: pd.DataFrame, session) -> bool:
    """Carga datos a tabla temporal de staging"""
    try:

        # 1. Eliminar tabla temporal si existe (evita problemas con if_exists='replace')
        session.execute(text("DROP TABLE IF EXISTS pg_temp.stg_fact_servicio"))
        session.commit()

        # 2. Crear tabla temporal
        session.execute(text("""
        CREATE TEMPORARY TABLE stg_fact_servicio (
            servicio_id INTEGER NOT NULL,
            cliente_id INTEGER NOT NULL,
            mensajero_id INTEGER,
            sede_id INTEGER NOT NULL,
            tipo_servicio_id INTEGER NOT NULL,
            estado_servicio_final_id INTEGER NOT NULL,
            prioridad VARCHAR(50),
            ciudad_origen VARCHAR(50),
            departamento_origen VARCHAR(50),
            ciudad_destino VARCHAR(50),
            departamento_destino VARCHAR(50),
            fecha_solicitud DATE,
            hora_solicitud TIME,
            fecha_iniciado DATE,
            hora_iniciado TIME,
            fecha_asignacion DATE,
            hora_asignacion TIME,
            fecha_recogida DATE,
            hora_recogida TIME,
            fecha_entrega DATE,
            hora_entrega TIME,
            fecha_cierre DATE,
            hora_cierre TIME
        ) ON COMMIT PRESERVE ROWS;
        """))

        # 3. Cargar datos
        df.to_sql(
            'stg_fact_servicio',
            session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )

        logger.info(f"Staging stg_fact_servicio completado ({len(df)} filas)")
        return True
    
    except Exception as e:
        logger.error(f"Error creando tabla stg_fact_servicio: {str(e)}", exc_info=True)
        return False
