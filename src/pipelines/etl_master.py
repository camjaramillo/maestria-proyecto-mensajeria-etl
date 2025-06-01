# src/pipelines/etl_master.py
from typing import Dict, List
from utils.logger import logger

# Configuración de pipelines
PIPELINES = {
    'dim_cliente': {
        'module': 'pipelines.etl_dim_cliente',
        'dependencies': []  # No depende de otras dimensiones
    },
    'dim_mensajero': {
        'module': 'pipelines.etl_dim_mensajero',
        'dependencies': []  # No depende de otras dimensiones
    },
    'dim_novedad': {
        'module': 'pipelines.etl_dim_novedad',
        'dependencies': []  # No depende de otras dimensiones
    },
    'dim_estado_servicio': {
        'module': 'pipelines.etl_dim_estado_servicio',
        'dependencies': []  # No depende de otras dimensiones
    },
    'dim_tipo_servicio': {
        'module': 'pipelines.etl_dim_tipo_servicio',
        'dependencies': []  # No depende de otras dimensiones
    },
    'dim_sede': {
        'module': 'pipelines.etl_dim_sede',
        'dependencies': []  # No depende de otras dimensiones
    },
    'dim_fecha': {
        'module': 'pipelines.etl_dim_fecha',
        'dependencies': []
    },
    'dim_hora': {
        'module': 'pipelines.etl_dim_hora',
        'dependencies': []
    }
    ##'fact_servicio': {
    ##    'module': 'pipelines.etl_fact_servicio',
    ##    'dependencies': ['dim_cliente', 'dim_fecha']  # Depende de estas dimensiones
    ##}
}

def execute(pipelines_to_run: List[str] = None) -> Dict[str, bool]:
    """
    Orquesta la ejecución de múltiples pipelines
    
    Args:
        pipelines_to_run: Lista de pipelines a ejecutar (None = todos)
    Returns:
        Diccionario con {nombre_pipeline: éxito_booleano}
    """
    results = {}
    targets = pipelines_to_run or PIPELINES.keys()  # Ejecutar todos si no se especifica
    
    for name in targets:
        # Verificar dependencias
        missing_deps = [
            dep for dep in PIPELINES[name]['dependencies'] 
            if not results.get(dep, False)
        ]
        
        if missing_deps:
            logger.error(f"⏭Saltando {name}. Dependencias faltantes: {missing_deps}")
            results[name] = False
            continue
            
        # Importar y ejecutar el pipeline
        try:
            module = __import__(
                PIPELINES[name]['module'], 
                fromlist=['run_etl']
            )
            success, _ = module.run_etl(truncate=True)
            results[name] = success
            logger.info(f"{'SUCCESS' if success else 'ERROR'} {name.upper()}")
            
        except Exception as e:
            logger.error(f"Error en {name}: {str(e)}", exc_info=True)
            results[name] = False
            
    return results