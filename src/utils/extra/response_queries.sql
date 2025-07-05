-- Pregunta 1
SELECT
  df.anio,
  df.mes,
  df.nombre_mes,
  COUNT(*) AS servicios_solicitados
FROM public.fact_servicio fs
JOIN public.dim_fecha df
  ON fs.fecha_solicitud_key = df.fecha_key
GROUP BY
  df.anio,
  df.mes,
  df.nombre_mes
ORDER BY
  df.anio,
  df.mes;
-------------------------------------------------------

