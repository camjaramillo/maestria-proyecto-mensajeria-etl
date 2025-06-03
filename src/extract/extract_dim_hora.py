import pandas as pd
from datetime import time
from typing import Tuple
from utils.logger import logger

def run_extract() -> Tuple[pd.DataFrame, bool]:
    try:
        # Generar una lista de tiempos para cada minuto del día
        times = [time(hour=h, minute=m) for h in range(24) for m in range(60)]
        
        # Extraer la hora y el minuto de cada tiempo
        horas_enteras = [t.hour for t in times]
        minutos = [t.minute for t in times]
        
        # Definir la franja horaria basada en la hora
        def obtener_franja(hora):
            if 0 <= hora <= 5:
                return 'Madrugada'
            elif 6 <= hora <= 11:
                return 'Mañana'
            elif 12 <= hora <= 17:
                return 'Tarde'
            else:
                return 'Noche'
        
        franjas = [obtener_franja(h) for h in horas_enteras]
        
        # Crear el DataFrame
        df = pd.DataFrame({
            'hora': times,
            'hora_entera': horas_enteras,
            'minuto': minutos,
            'franja_horaria': franjas
        })
        
        logger.info(f"Extracción de dim_hora completada: {len(df)} registros generados.")
        return df, True
    except Exception as e:
        logger.error(f"Error durante la extracción de dim_hora: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
