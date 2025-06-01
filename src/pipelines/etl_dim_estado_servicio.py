# src/pipelines/etl_dim_cliente.py
from typing import Tuple
from pipelines.etl_base import run_etl_with_staging
from extract.extract_dim_estado_servicio import run_extract
from staging.staging_dim_estado_servicio import run_staging
from transform.transform_dim_estado_servicio import run_transform
from load.load_dim_estado_servicio import run_load

def run_etl(truncate: bool = True) -> Tuple[bool, int]:
    """Orquesta el ETL completo para DimNovedad"""
    return run_etl_with_staging(
        extract_fn=run_extract,
        staging_fn=run_staging,
        transform_fn=run_transform,
        load_fn=run_load,
        table_name="dim_estado_servicio",
        truncate=truncate
    )