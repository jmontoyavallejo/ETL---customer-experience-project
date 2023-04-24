CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.prd_mineria_almacen` AS
SELECT
  CAST (id AS int64) id,
  CAST (id_pvd AS int64) id_pvd,
  formato,
  ciudad_pdv,
  estado_pdv,
  zona,
  clima,
  CAST (REPLACE(REPLACE(area_m2, ',', '.'),'CERRADA',NULL) AS float64) area_m2,
  eq_cod,
  negocio,
  CAST (REPLACE(cargue, '-', NULL) AS int64) cargue,
  CAST (REPLACE(cargue_unidades, '-', NULL) AS int64) cargue_unidades,
  formato2,
  clase_tienda,
  canal,
  pdv,
  departamento_pdv,
  correo_pdv
FROM
  `customer-experience-384423.Data_cruda.mineria_almacen`
