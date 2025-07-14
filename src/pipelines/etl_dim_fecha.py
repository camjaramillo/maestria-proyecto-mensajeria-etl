from typing import Tuple
from pipelines.etl_base import run_etl_with_staging
from extract.extract_dim_fecha import run_extract
from staging.staging_dim_fecha import run_staging
from transform.transform_dim_fecha import run_transform
from load.load_dim_fecha import run_load
from utils.database import DBConnection

def run_etl(truncate: bool = True, start_date = None) -> Tuple[bool, int]:
    """Orquesta el ETL completo para DimFecha"""
    return run_etl_with_staging(
        extract_fn=lambda: run_extract(start_date=start_date),
        staging_fn=run_staging,
        transform_fn=run_transform,
        load_fn=run_load,
        table_name="dim_fecha",
        truncate=truncate
    )
