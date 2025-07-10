from typing import Tuple
from pipelines.etl_base import run_etl_with_staging
from extract.extract_fact_estado_servicio_transaccional import run_extract
from staging.staging_fact_estado_servicio_transaccional import run_staging
from transform.transform_fact_estado_servicio_transaccional import run_transform
from load.load_fact_estado_servicio_transaccional import run_load

def run_etl(truncate: bool = True) -> Tuple[bool, int]:
    return run_etl_with_staging(
        extract_fn=run_extract,
        staging_fn=run_staging,
        transform_fn=run_transform,
        load_fn=run_load,
        table_name="fact_estado_servicio_transaccional",
        truncate=truncate
    )