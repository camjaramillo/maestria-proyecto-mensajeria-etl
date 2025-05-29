import pandas as pd

def transform_dim_cliente(df_cliente, df_ciudad, df_departamento):
    """
    Transforma y combina los datos de cliente, ciudad y departamento para construir la dimensión DimCliente.

    Parámetros:
    - df_cliente (DataFrame): Datos extraídos de la tabla cliente.
    - df_ciudad (DataFrame): Datos extraídos de la tabla ciudad.
    - df_departamento (DataFrame): Datos extraídos de la tabla departamento.

    Retorna:
    - DataFrame: Datos transformados listos para cargar en la dimensión DimCliente.
    """

    # Combinar cliente con ciudad
    df = pd.merge(df_cliente, df_ciudad, how="left", on="ciudad_id", suffixes=('_cliente', '_ciudad'))

    # Combinar el resultado anterior con departamento
    df = pd.merge(df, df_departamento, how="left", on="departamento_id", suffixes=('_cliente', '_departamento'))

    # Seleccionar y renombrar columnas relevantes
    df_final = df[[
        "cliente_id",
        "nit_cliente",
        "nombre_cliente",
        "email",
        "direccion",
        "telefono",
        "ciudad_id",
        "nombre_ciudad",
        "nombre_departamento",
        "activo",
        "sector"
    ]].rename(columns={
        "nit_cliente": "nit",
        "nombre_cliente": "nombre",
        "nombre_ciudad": "ciudad",
        "nombre_departamento": "departamento"
    })

    # Opcional: ordenar las columnas
    columnas_ordenadas = [
        "cliente_id",
        "nit_cliente",
        "nombre_cliente",
        "email",
        "direccion",
        "telefono",
        "nombre_contacto",
        "ciudad",
        "departamento",
        "activo",
        "sector"
    ]
    df_final = df_final[columnas_ordenadas]

    return df_final
