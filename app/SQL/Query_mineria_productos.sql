CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.prd_mineria_productos` AS
SELECT
  CAST (REPLACE(id,'.','') AS int64) id,
  genero,
  personaje,
  marca,
  marca_unificada,
  silueta,
  sublinea_exito,
  tipo_de_prenda,
  prenda,
  mes_coleccion,
  ano_coleccion,
  ropero,
  tipo_inv,
  estrategias,
  comerciales,
  categoria,
  coleccion,
  espacios,
  contador,
  linea,
  licencia,
  CAST (REPLACE(id_referencia,'.','') AS int64) id_referencia,
  talla_orden,
  cod_color,
  descripcion,
  grupo,
  CAST (REPLACE(replace(referencia,
        '.',
        ''),'mug',NULL) AS int64) referencia
FROM
  `customer-experience-384423.Data_cruda.mineria_producto
