from sqlalchemy import text
import pandas as pd
from utils.logger import logger

def run_staging(df: pd.DataFrame, staging_session, target_session = None) -> bool:
    """Carga datos a tabla permanente en STAGING y copia temporal en TARGET (misma sesión)"""
    try:
        table_name = 'stg_fact_servicio'

        # 1. Crear tabla permanente en STAGING
        staging_session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        staging_session.execute(text(f"""
        CREATE TABLE {table_name} (
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """))
        staging_session.commit()

        df.to_sql(
            table_name,
            staging_session.connection(),
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        logger.info(f"Staging STAGING.{table_name} completado ({len(df)} filas)")

        # 2. Crear tabla TEMPORAL en TARGET (solo si se proporcionó sesión)
        if target_session is not None:
            target_session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            target_session.execute(text(f"""
            CREATE TEMPORARY TABLE {table_name} (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ON COMMIT PRESERVE ROWS
            """))
            target_session.commit()

            df.to_sql(
                table_name,
                target_session.connection(),
                if_exists='append',
                index=False,
                method='multi',
                chunksize=1000
            )
            logger.info(f"Staging TARGET.TEMP {table_name} completado ({len(df)} filas)")

        return True

    except Exception as e:
        logger.error(f"Error en staging {table_name}: {str(e)}", exc_info=True)
        return False
