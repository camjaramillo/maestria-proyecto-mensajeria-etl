from sqlalchemy import text
import pandas as pd
from utils.logger import logger
from typing import Tuple

def run_transform(session) -> Tuple[pd.DataFrame, bool]:
    """Transforma datos desde staging y calcula métricas"""
    try:
        query = text("""
        WITH 
        tiempo_dim_fecha AS (
            SELECT
                fecha_key,
                fecha
            FROM dim_fecha
        ),
        tiempo_dim_hora AS (
            SELECT
                hora_key,
                hora
            FROM dim_hora
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY servicio_id) AS servicio_key,
            servicio_id,
            dc.cliente_key,
            dm.mensajero_key,
            ds.sede_key,
            dts.tipo_servicio_key,
            des.estado_servicio_key AS estado_servicio_final_key,
            CASE 
                WHEN UPPER(prioridad) LIKE '%ALTA%' THEN 'ALTA'
                WHEN UPPER(prioridad) LIKE '%MEDIA%' THEN 'MEDIA'
                ELSE 'BAJA'
            END AS prioridad,         
			UPPER(TRIM(sfs.ciudad_origen)) AS ciudad_origen,
			UPPER(TRIM(sfs.departamento_origen)) AS departamento_origen,
			UPPER(TRIM(sfs.ciudad_destino)) AS ciudad_destino,
			UPPER(TRIM(sfs.departamento_destino)) AS departamento_destino,
            tdf_solicitud.fecha_key AS fecha_solicitud_key,
            tdh_solicitud.hora_key AS hora_solicitud_key,
            tdf_iniciado.fecha_key AS fecha_iniciado_key,
            tdh_iniciado.hora_key AS hora_iniciado_key,
            tdf_asignacion.fecha_key AS fecha_asignacion_key,
            tdh_asignacion.hora_key AS hora_asignacion_key,
            tdf_recogida.fecha_key AS fecha_recogida_key,
            tdh_recogida.hora_key AS hora_recogida_key,
            tdf_entrega.fecha_key AS fecha_entrega_key,
            tdh_entrega.hora_key AS hora_entrega_key,
            CASE
                WHEN fecha_solicitud IS NOT NULL AND hora_solicitud IS NOT NULL
                    AND fecha_entrega IS NOT NULL AND hora_entrega IS NOT NULL THEN
                    ROUND(
                        EXTRACT(EPOCH FROM( (fecha_entrega + hora_entrega) - (fecha_solicitud + hora_solicitud) )) / 60
                    )
                ELSE NULL
            END AS tiempo_total_entrega_min
            FROM 
                pg_temp.stg_fact_servicio sfs
                -- DimCliente
                LEFT JOIN dim_cliente dc ON sfs.cliente_id = dc.cliente_id
                -- DimMensajero
                LEFT JOIN dim_mensajero dm ON sfs.mensajero_id = dm.mensajero_id
                -- DimSede		
                LEFT JOIN dim_sede ds ON sfs.sede_id = ds.sede_id
                -- DimTipoServicio
                LEFT JOIN dim_tipo_servicio dts ON sfs.tipo_servicio_id = dts.tipo_servicio_id
                -- DimEstadoServicio
                LEFT JOIN dim_estado_servicio des ON sfs.estado_servicio_final_id = des.estado_servicio_id
                -- DimFecha
                LEFT JOIN tiempo_dim_fecha tdf_solicitud ON sfs.fecha_solicitud = tdf_solicitud.fecha
                LEFT JOIN tiempo_dim_fecha tdf_iniciado ON sfs.fecha_iniciado = tdf_iniciado.fecha
                LEFT JOIN tiempo_dim_fecha tdf_asignacion ON sfs.fecha_asignacion = tdf_asignacion.fecha
                LEFT JOIN tiempo_dim_fecha tdf_recogida ON sfs.fecha_recogida = tdf_recogida.fecha
                LEFT JOIN tiempo_dim_fecha tdf_entrega ON sfs.fecha_entrega = tdf_entrega.fecha
                -- DimHora
                LEFT JOIN tiempo_dim_hora tdh_solicitud ON sfs.hora_solicitud = tdh_solicitud.hora 
                LEFT JOIN tiempo_dim_hora tdh_iniciado ON sfs.hora_iniciado = tdh_iniciado.hora
                LEFT JOIN tiempo_dim_hora tdh_asignacion ON sfs.hora_asignacion = tdh_asignacion.hora
                LEFT JOIN tiempo_dim_hora tdh_recogida ON sfs.hora_recogida = tdh_recogida.hora
                LEFT JOIN tiempo_dim_hora tdh_entrega ON sfs.hora_entrega = tdh_entrega.hora
        """)
        df = pd.read_sql(query, session.connection())
        logger.info(f"Transformación completada: {len(df)} filas")
        return df, True
    except Exception as e:
        logger.error(f"Error en transformación: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
