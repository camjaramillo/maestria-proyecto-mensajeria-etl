# src/utils/logger.py
import logging
from pathlib import Path
from datetime import datetime

def setup_logger(name: str = "etl_pipeline", log_level: str = "INFO") -> logging.Logger:
    """
    Configura y retorna un logger configurado.
    """
    logger = logging.getLogger(name)
    
    # Evita agregar handlers múltiples si ya existen
    if not logger.handlers:
        logger.setLevel(log_level.upper())
        
        # Formato
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Handler de consola
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
        # Handler de archivo
        logs_dir = Path(__file__).parent.parent / "logs"
        logs_dir.mkdir(exist_ok=True)
        
        log_file = logs_dir / f"etl_{datetime.now().strftime('%Y-%m-%d')}.log"
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    
    return logger

# Instancia global del logger
logger = setup_logger()