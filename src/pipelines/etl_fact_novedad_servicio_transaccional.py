from typing import Tuple
from pipelines.etl_base import run_etl_with_staging
from extract.extract_fact_novedad_servicio_transaccional import run_extract
from staging.staging_fact_novedad_servicio_transaccional import run_staging
from transform.transform_fact_novedad_servicio_transaccional import run_transform
from load.load_fact_novedad_servicio_transaccional import run_load
from utils.database import DBConnection

def run_etl(truncate: bool = True, start_date = None) -> Tuple[bool, int]:
    return run_etl_with_staging(
        extract_fn=lambda: run_extract(start_date=start_date),
        staging_fn=run_staging,
        transform_fn=run_transform,
        load_fn=run_load,
        table_name="fact_novedad_servicio_transaccional",
        truncate=truncate,
        transform_connection=DBConnection.TARGET
    )