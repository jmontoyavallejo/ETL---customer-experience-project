CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.prd_mineria_ventas` AS
SELECT
  CAST (`ID FACTURA` AS int64) id_factura,
    CASE
    WHEN REGEXP_CONTAINS(FECHA_LARGA, r'T') AND LENGTH(FECHA_LARGA) > 10 THEN FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', FECHA_LARGA))
    WHEN NOT REGEXP_CONTAINS(FECHA_LARGA, r'T') AND LENGTH(FECHA_LARGA) > 10 THEN FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E*S', FECHA_LARGA))
    ELSE FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_ADD(TIMESTAMP_MICROS((CAST(FECHA_LARGA AS INT64) - 25569) * 86400000000), INTERVAL 2 HOUR))
  END AS fecha_larga,
  `ID_PDV` id_pdv,
  `ID CLIENTE` id_cliente,
  `NUMERO FACTURA` numero_factura,
  `UNIDADES` unidades,
  `PORCENTAJE DESCUENTO` porcentaje_descuento,
  `PRECIO FULL` precio_full,
  `PORCENTAJE IVA` porcentaje_iva,
  `PRECIO ANTES IVA` precio_antes_iva,
  `PRECIO CON IVA` precio_con_iva,
  `FORMAS DE PAGO` formas_de_pago,
  `ID REFERENCIA` id_referencia,
  `PROMOCION` promocion,
  `HABEAS DATA VENTA` habeas_data_venta,
  `VALOR DESCUENTO` valor_descuento,
  `COSTO TOTAL` costo_total,
  `COSTO UNITARIO` costo_unitario,
  `ID_VENDEDOR` id_vendedor,
  `ESTADO ID VENTA` estado_id_venta,
  `ESTADO CELULAR VENTA` estado_celular_venta,
  `ESTADO EMAIL VENTA` estado_email_venta,
  `FECHA` fecha,
  `CONSECUTIVO TRX` consecutivo_trx,
  `CAPTURA DE DATOS CEL` captura_de_datos_cel,
  `CAPTURA DE DATOS CEL+CORREO` captura_de_datos_celcorreo,
  `CAPTURA DE DATOS COMPLETO` captura_de_datos_completo,
  `RANGO TICKET` rango_ticket,
  `TICKET` ticket
FROM
  `customer-experience-384423.Data_cruda.mineria_venta`