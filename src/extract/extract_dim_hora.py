import pandas as pd
from datetime import time
from typing import Tuple
from utils.logger import logger

def run_extract() -> Tuple[pd.DataFrame, bool]:
    try:
        horas = list(range(24))
        
        def obtener_franja(hora):
            if 0 <= hora <= 5:
                return 'Madrugada'
            elif 6 <= hora <= 11:
                return 'Mañana'
            elif 12 <= hora <= 17:
                return 'Tarde'
            else:
                return 'Noche'
        
        df = pd.DataFrame({
            'hora': [time(h) for h in horas],
            'hora_entera': horas,
            'minuto': [0]*24,
            'franja_horaria': [obtener_franja(h) for h in horas]
        })
        
        logger.info(f"Extracción de dim_hora completada: {len(df)} registros generados.")
        return df, True
    except Exception as e:
        logger.error(f"Error durante la extracción de dim_hora: {str(e)}", exc_info=True)
        return pd.DataFrame(), False
