from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple
from utils.database import db_session, DBConnection

def run_extract(start_date: str = None) -> Tuple[pd.DataFrame, bool]:
    """
    Extrae los datos necesarios desde la base de datos de origen y retorna un DataFrame.
    """
    try:
        query = text("""
        WITH 
        estado_iniciado AS (
            SELECT servicio_id,
                fecha AS fecha_iniciado,
                DATE_TRUNC('minute', hora + INTERVAL '30 seconds')::TIME AS hora_iniciado
            FROM (
            SELECT servicio_id, fecha, hora,
                    ROW_NUMBER() OVER (PARTITION BY servicio_id ORDER BY fecha, hora) AS rn
            FROM mensajeria_estadosservicio
            WHERE estado_id = 1
            ) t
            WHERE t.rn = 1
        ),
        estado_asignacion AS (
            SELECT servicio_id,
                fecha AS fecha_asignacion,
                DATE_TRUNC('minute', hora + INTERVAL '30 seconds')::TIME AS hora_asignacion
            FROM (
            SELECT servicio_id, fecha, hora,
                    ROW_NUMBER() OVER (PARTITION BY servicio_id ORDER BY fecha, hora) AS rn
            FROM mensajeria_estadosservicio
            WHERE estado_id = 2
            ) t
            WHERE t.rn = 1
        ),
        estado_recogida AS (
            SELECT servicio_id,
                fecha AS fecha_recogida,
                DATE_TRUNC('minute', hora + INTERVAL '30 seconds')::TIME AS hora_recogida
            FROM (
            SELECT servicio_id, fecha, hora,
                    ROW_NUMBER() OVER (PARTITION BY servicio_id ORDER BY fecha, hora) AS rn
            FROM mensajeria_estadosservicio
            WHERE estado_id = 4
            ) t
            WHERE t.rn = 1
        ),
        estado_entrega AS (
            SELECT servicio_id,
                fecha AS fecha_entrega,
                DATE_TRUNC('minute', hora + INTERVAL '30 seconds')::TIME AS hora_entrega
            FROM (
            SELECT servicio_id, fecha, hora,
                    ROW_NUMBER() OVER (PARTITION BY servicio_id ORDER BY fecha, hora) AS rn
            FROM mensajeria_estadosservicio
            WHERE estado_id = 5
            ) t
            WHERE t.rn = 1
        ),
        estado_final AS (
            SELECT servicio_id,
                estado_id AS estado_servicio_final_id
            FROM (
            SELECT servicio_id, estado_id,
                    ROW_NUMBER() OVER (PARTITION BY servicio_id ORDER BY fecha DESC, hora DESC) AS rn
            FROM mensajeria_estadosservicio
            ) t
            WHERE t.rn = 1
        ),
        origen_servicio AS (
            SELECT mos.id        AS origen_id,
                ci.nombre     AS ciudad_origen,
                d.nombre      AS departamento_origen
            FROM mensajeria_origenservicio mos
            JOIN ciudad ci ON mos.ciudad_id = ci.ciudad_id
            JOIN departamento d ON ci.departamento_id = d.departamento_id
        ),
        destino_servicio AS (
            SELECT mds.id       AS destino_id,
                ci.nombre    AS ciudad_destino,
                d.nombre     AS departamento_destino
            FROM mensajeria_destinoservicio mds
            JOIN ciudad ci ON mds.ciudad_id = ci.ciudad_id
            JOIN departamento d ON ci.departamento_id = d.departamento_id
        )
        SELECT
            s.id AS servicio_id,
            s.cliente_id,
            s.mensajero_id,
            cu.sede_id,
            s.tipo_servicio_id,
            ef.estado_servicio_final_id,
            s.prioridad,
            os.ciudad_origen,
            os.departamento_origen,
            ds.ciudad_destino,
            ds.departamento_destino,
            s.fecha_solicitud,
            DATE_TRUNC('minute', s.hora_solicitud + INTERVAL '30 seconds')::TIME AS hora_solicitud,
            ei.fecha_iniciado,
            ei.hora_iniciado,
            ea.fecha_asignacion,
            ea.hora_asignacion,
            er.fecha_recogida,
            er.hora_recogida,
            et.fecha_entrega,
            et.hora_entrega
        FROM mensajeria_servicio s
        -- solo se obtienen los servicios que tienen cada uno de estos cuatro estados (se evitan nulls)
        INNER JOIN estado_iniciado   ei ON ei.servicio_id   = s.id
        INNER JOIN estado_asignacion ea ON ea.servicio_id   = s.id
        INNER JOIN estado_recogida   er ON er.servicio_id   = s.id
        INNER JOIN estado_entrega    et ON et.servicio_id   = s.id
        -- Left joins
        LEFT  JOIN estado_final      ef ON ef.servicio_id   = s.id
        LEFT  JOIN origen_servicio   os ON os.origen_id     = s.origen_id
        LEFT  JOIN destino_servicio  ds ON ds.destino_id    = s.destino_id
        LEFT  JOIN clientes_usuarioaquitoy cu ON cu.id       = s.usuario_id
        WHERE
            s.es_prueba = FALSE
            AND s.fecha_solicitud IS NOT NULL
            AND s.hora_solicitud IS NOT NULL
            AND s.fecha_solicitud >= :start_date
        ORDER BY s.id;
        """)

        with db_session(DBConnection.SOURCE) as session:
            df = pd.read_sql(query, session.connection(), params={"start_date": start_date})
            return df, not df.empty

    except Exception as e:
        logger.error("Error extrayendo los datos para fact_servicio", exc_info=True)
        return pd.DataFrame(), False
