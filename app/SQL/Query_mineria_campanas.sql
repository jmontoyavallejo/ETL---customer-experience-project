CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.Prd_resultado_campanas` AS
SELECT
  CAST(id_cliente AS int64) id_cliente,
  key_movimiento,
  codigo_campana,
  CAST(codigo_campana_master AS int64 ) codigo_campana_master,
  estado,
  nombre_campana,
  fecha_inicio,
  fecha_fin,
  campo_fecha,
  variable_1,
  detalle_variable_1,
  filtro_general_1,
  variable_2,
  detalle_variable_2,
  filtro_general_2,
  variable_3,
  detalle_variable_3,
  filtro_general_3,
  copy,
  cliente_contactado,
  tipo_contacto,
  rango_compra_historica,
  rango_recencia,
  rfm,
  rfm_segmento,
  campana_efectiva_tipo,
  mantener_campana
FROM
  `customer-experience-384423.Data_cruda.mineria_resultado_campanas`