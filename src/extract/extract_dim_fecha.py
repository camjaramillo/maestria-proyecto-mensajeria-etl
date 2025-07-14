import pandas as pd
from sqlalchemy import text
from utils.database import db_session, DBConnection
from utils.logger import logger
from datetime import datetime
from typing import Tuple
import holidays
from utils.date_translations import MONTHS_EN_ES, DAYS_EN_ES

def run_extract(start_date: str = None) -> Tuple[pd.DataFrame, bool]:
    """
    Genera un DataFrame de fechas desde el 1 de enero del año de la fecha mínima
    hasta el 31 de diciembre del año de la fecha máxima en la tabla mensajeria_servicio.
    Marca los días festivos en Colombia y traduce los nombres de meses y días al español.
    """
    try:
        # Establecer sesión con la base de datos de origen
        with db_session(DBConnection.SOURCE) as session:
            # Obtener fecha mínima y máxima de la tabla mensajeria_servicio
            result = session.execute(text("""
                SELECT 
                    MIN(fecha_solicitud) AS min_fecha, 
                    MAX(fecha_solicitud) AS max_fecha
                FROM mensajeria_servicio
            """)).fetchone()

            min_fecha = result.min_fecha
            max_fecha = result.max_fecha

            if not min_fecha or not max_fecha:
                logger.error("No se encontraron fechas en la tabla mensajeria_servicio.")
                return pd.DataFrame(), False

            # Ajustar fechas al 1 de enero y 31 de diciembre de los años correspondientes
            start_date = datetime(min_fecha.year, 1, 1)
            end_date = datetime(max_fecha.year, 12, 31)

            # Generar rango de fechas
            fechas = pd.date_range(start=start_date, end=end_date, freq='D')

            # Obtener festivos de Colombia para los años en el rango
            years = list(range(start_date.year, end_date.year + 1))
            festivos_colombia = holidays.Colombia(years=years)

            # Construir DataFrame
            df = pd.DataFrame({
                'fecha': fechas,
                'anio': fechas.year,
                'mes': fechas.month,
                'dia': fechas.day,
                'trimestre': fechas.quarter,
                'nombre_mes': fechas.strftime('%B').map(MONTHS_EN_ES),
                'dia_semana': fechas.strftime('%A').map(DAYS_EN_ES),
                'es_festivo': fechas.isin(festivos_colombia)
            })

            logger.info(f"Extracción de fechas completada: {len(df)} registros generados.")
            return df, True

    except Exception as e:
        logger.error(f"Error durante la extracción de fechas: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
