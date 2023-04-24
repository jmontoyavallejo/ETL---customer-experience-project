CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.Prd_resultado_campanas` AS
SELECT
  id_factura,
  CASE
    WHEN REGEXP_CONTAINS(fecha_larga, r'T') AND LENGTH(fecha_larga) > 10 THEN FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', fecha_larga))
    WHEN NOT REGEXP_CONTAINS(fecha_larga, r'T')
  AND LENGTH(fecha_larga) > 10 THEN FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', fecha_larga))
  ELSE
  FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_ADD(TIMESTAMP_MICROS((CAST(fecha_larga AS INT64) - 25569) * 86400000000), INTERVAL 2 HOUR))
END
  AS fecha_larga,
  id_pdv,
  CAST(id_cliente AS int64) id_cliente,
  numero_factura,
  UNIDADES unidades,
  porcentaje_descuento,
  precio_full,
  CAST (REPLACE(LEFT(porcentaje_iva, LENGTH(porcentaje_iva) - 5),'.','') AS int64)/100 porcentaje_iva,
  CAST(REPLACE(REPLACE(REPLACE(precio_antes_iva,'.',''),'$',''),' ','') AS INT64) AS precio_antes_iva,
  precio_con_iva,
  formas_de_pago,
  CAST (id_referencia AS int64) id_referencia,
  promocion,
  habeas_data_venta,
  valor_descuento,
  costo_total,
  costo_unitario,
  id_vendedor,
  estado_id_venta,
  estado_celular_venta,
  estado_email_venta,
  CASE
    WHEN REGEXP_CONTAINS(fecha,r'T') AND LENGTH(fecha) > 10 THEN FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', fecha))
    WHEN NOT REGEXP_CONTAINS(fecha, r'T')
  AND LENGTH(fecha) > 10 THEN FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', fecha))
  ELSE
  FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_ADD(TIMESTAMP_MICROS((CAST(fecha AS INT64) - 25569) * 86400000000), INTERVAL 2 HOUR))
END
  AS fecha,
  consecutivo_trx,
  captura_de_datos_cel,
  captura_de_datos_cel_correo,
  captura_de_datos_completo,
  rango_ticket,
  CAST(ticket AS float64) ticket
FROM
  `customer-experience-384423.Data_cruda.mineria_resultado_campanas`
