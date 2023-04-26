CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.Prd_clientes_simplificado` AS
WITH
  promedio AS (
  SELECT
    id_cliente,
    AVG(ticket ) promedio_compra,
    COUNT(*) cantidad_compras,
    AVG(ticket )/COUNT(*) dropsize
  FROM
    `customer-experience-384423.Data_refinada.prd_mineria_ventas`
  GROUP BY
    1 ),
  clientes_contactable AS (
  SELECT
    id_cliente,
    CASE
      WHEN MAX(habeas_data_venta) = 'SI' AND (MAX(estado_celular_venta) = 'SI' OR MAX(estado_email_venta) = 'SI' AND id_cliente NOT IN ( SELECT id FROM `customer-experience-384423.Data_refinada.prd_blacklist` )) THEN 1
    ELSE
    0
  END
    AS contactable
  FROM
    `customer-experience-384423.Data_refinada.prd_mineria_ventas`
  GROUP BY
    id_cliente ),
  puntos_venta AS(
  SELECT
    t1.id_cliente,
    MAX(CASE
        WHEN t2.formato = 'OUTLET' THEN 1
      ELSE
      0
    END
      ) AS compro_en_OUTLET,
    MAX(CASE
        WHEN t2.formato = 'ESTELAR' THEN 1
      ELSE
      0
    END
      ) AS compro_en_ESTELAR,
    MAX(CASE
        WHEN t2.formato = 'MOVIES' THEN 1
      ELSE
      0
    END
      ) AS compro_en_MOVIES,
    MAX(CASE
        WHEN t2.formato = 'LITTLE MIC' THEN 1
      ELSE
      0
    END
      ) AS compro_en_LITTLE_MIC,
    MAX(CASE
        WHEN t2.formato = 'MOVIES-W' THEN 1
      ELSE
      0
    END
      ) AS compro_en_MOVIES_W,
    MAX(CASE
        WHEN t2.formato = 'OUTLET MIC' THEN 1
      ELSE
      0
    END
      ) AS compro_en_OUTLET_MIC,
    MAX(CASE
        WHEN t2.formato = 'EVENTOS' THEN 1
      ELSE
      0
    END
      ) AS compro_en_EVENTOS,
    MAX(CASE
        WHEN t2.formato = 'OUTLET LITTLE MIC' THEN 1
      ELSE
      0
    END
      ) AS compro_en_OUTLET_LITTLE_MIC,
    MAX(CASE
        WHEN t2.formato = 'ESTELAR ECUADOR' THEN 1
      ELSE
      0
    END
      ) AS compro_en_ESTELAR_ECUADOR,
    MAX(CASE
        WHEN t2.formato = 'LITTLE MIC ECUADOR' THEN 1
      ELSE
      0
    END
      ) AS compro_en_LITTLE_MIC_ECUADOR
  FROM
    `customer-experience-384423.Data_refinada.prd_mineria_ventas` t1
  LEFT JOIN
    `customer-experience-384423.Data_refinada.prd_mineria_almacen` t2
  ON
    t1.id_pdv = t2.id_pvd
  GROUP BY
    t1.id_cliente ),
  ultima_fecha AS(
  SELECT
    id_cliente,
    MAX(CAST(fecha AS DATE)) AS ultima_fecha_compra,
    CAST(DATE_DIFF(CURRENT_DATE(), MAX(CAST(fecha AS DATE)), DAY)AS int64) AS dias_desde_ultima_compra
  FROM
    `customer-experience-384423.Data_refinada.prd_mineria_ventas`
  GROUP BY
    id_cliente),
  historial_compras AS(
  WITH
    basepivot AS (
    SELECT
      id_cliente,
      t2.tipo_de_prenda tipo_de_prenda,
      SUM(ticket) amount
    FROM
      `customer-experience-384423.Data_refinada.prd_mineria_ventas` AS t1
    INNER JOIN
      `customer-experience-384423.Data_refinada.prd_mineria_productos` AS t2
    ON
      t1.id_referencia=t2.id_referencia
    GROUP BY
      1,
      2)
  SELECT
    *
  FROM
    basepivot PIVOT(SUM(amount) FOR tipo_de_prenda IN ( 'ELEFANTES',
        'PELUCHES',
        'CALCETINES',
        'PELUCHE',
        'BOLSO',
        'CARTUCHERA',
        'BILLETERA',
        'FUNKOS',
        'SALIDA_DE_BANO',
        'GAFAS',
        'SET_DE_ALIMENTACION',
        'JUGUETERIA',
        'CARTERA',
        'MALETA_VIAJERA',
        'TENIS_CON_RUEDAS',
        'SET_DE_BELLEZA',
        'SUECOS',
        'MORRAL',
        'PANTUFLAS',
        'PARAGUAS',
        'LONCHERA',
        'SANDALIAS',
        'BOTAS',
        'BONO',
        'MALETA_CON_RUEDAS',
        'SET_ESCOLAR',
        'GORRAS',
        'GORRO',
        'COBIJA',
        'BABERO',
        'TENIS',
        'BALETA',
        'ZAPATOS',
        'TENIS_CON_LUCES',
        'CORREAS',
        'AROMAS',
        'POP',
        'BOLSA',
        'RELOJ',
        'MOCHILA',
        'ESTUCHES',
        'PANALERA',
        'DULCES',
        'CAMISETA',
        'SET',
        'STICKERS',
        'PANTALON',
        'ACCESORIOS')) ),historial_zonas as(SELECT
  t1.id_cliente,
  MAX(CASE
      WHEN t2.zona = 'FRANKY' THEN 1
    ELSE
    0
  END
    ) AS compro_en_FRANKY,
  MAX(CASE
      WHEN t2.zona = 'ZONA 1' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_1,
  MAX(CASE
      WHEN t2.zona = 'ZONA 2' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_2,
  MAX(CASE
      WHEN t2.zona = 'ZONA 3' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_3,
  MAX(CASE
      WHEN t2.zona = 'ZONA 4' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_4,
  MAX(CASE
      WHEN t2.zona = 'ZONA 5' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_5,
  MAX(CASE
      WHEN t2.zona = 'ZONA 6' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_6,
  MAX(CASE
      WHEN t2.zona = 'ZONA 7' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_7,
  MAX(CASE
      WHEN t2.zona = 'ZONA M' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ZONA_M,
  MAX(CASE
      WHEN t2.zona = 'ECUADOR' THEN 1
    ELSE
    0
  END
    ) AS compro_en_ECUADOR,
  MAX(CASE
      WHEN t2.zona = 'INTERNACIONAL' THEN 1
    ELSE
    0
  END
    ) AS compro_en_INTERNACIONAL
FROM
  `customer-experience-384423.Data_refinada.prd_mineria_ventas` t1
LEFT JOIN
  `customer-experience-384423.Data_refinada.prd_mineria_almacen` t2
ON
  t1.id_pdv = t2.id_pvd
GROUP BY
  t1.id_cliente),historial_compras_periodo as(
    WITH
  pivottable AS (
  WITH
    basepivot AS (
    SELECT
      id_cliente,
      EXTRACT(YEAR
      FROM
        CAST(fecha AS date) ) periodo,
      SUM(CAST(ticket AS float64)) promedio
    FROM
      `customer-experience-384423.Data_refinada.prd_mineria_ventas`
    GROUP BY
      1,
      2 )
  SELECT
    *
  FROM
    basepivot PIVOT (SUM(promedio) FOR periodo IN(2018,
        2019,
        2020,
        2021)))
SELECT
  id_cliente,
  _2018 compras_2018,
  _2019 compras_2019,
  _2020 compras_2020,
  _2021 compras_2021,
FROM
  pivottable
  )
SELECT
  t1.*,
  t2.contactable,
  t3.compro_en_OUTLET,
  t3.compro_en_ESTELAR,
  t3.compro_en_MOVIES,
  t3.compro_en_LITTLE_MIC,
  t3.compro_en_MOVIES_W,
  t3.compro_en_OUTLET_MIC,
  t3.compro_en_EVENTOS,
  t3.compro_en_OUTLET_LITTLE_MIC,
  t3.compro_en_ESTELAR_ECUADOR,
  t3.compro_en_LITTLE_MIC_ECUADOR,
  t4.dias_desde_ultima_compra dias_desde_ultima_compra_recencia,
  t5.ELEFANTES,
  t5.PELUCHES,
  t5.CALCETINES,
  t5.PELUCHE,
  t5.BOLSO,
  t5.CARTUCHERA,
  t5.BILLETERA,
  t5.FUNKOS,
  t5.SALIDA_DE_BANO,
  t5.GAFAS,
  t5.SET_DE_ALIMENTACION,
  t5.JUGUETERIA,
  t5.CARTERA,
  t5.MALETA_VIAJERA,
  t5.TENIS_CON_RUEDAS,
  t5.SET_DE_BELLEZA,
  t5.SUECOS,
  t5.MORRAL,
  t5.PANTUFLAS,
  t5.PARAGUAS,
  t5.LONCHERA,
  t5.SANDALIAS,
  t5.BOTAS,
  t5.BONO,
  t5.MALETA_CON_RUEDAS,
  t5.SET_ESCOLAR,
  t5.GORRAS,
  t5.GORRO,
  t5.COBIJA,
  t5.BABERO,
  t5.TENIS,
  t5.BALETA,
  t5.ZAPATOS,
  t5.TENIS_CON_LUCES,
  t5.CORREAS,
  t5.AROMAS,
  t5.POP,
  t5.BOLSA,
  t5.RELOJ,
  t5.MOCHILA,
  t5.ESTUCHES,
  t5.PANALERA,
  t5.DULCES,
  t5.CAMISETA,
  t5.SET,
  t5.STICKERS,
  t5.PANTALON,
  t5.ACCESORIOS,
  t6.compro_en_INTERNACIONAL,
  t6.compro_en_ZONA_1,
  t6.compro_en_ZONA_2,
  t6.compro_en_ZONA_3,
  t6.compro_en_ZONA_4,
  t6.compro_en_ZONA_5,
  t6.compro_en_ZONA_6,
  t6.compro_en_ZONA_7,
  t6.compro_en_ZONA_M,
  t6.compro_en_ECUADOR,
  t7.compras_2018,
  t7.compras_2019,
  t7.compras_2020,
  t7.compras_2021,
FROM
  promedio AS t1
INNER JOIN
  clientes_contactable AS t2
ON
  t1.id_cliente =t2.id_cliente
INNER JOIN
  puntos_venta AS t3
ON
  t1.id_cliente =t3.id_cliente
INNER JOIN
  ultima_fecha AS t4
ON
  t1.id_cliente =t4.id_cliente
INNER JOIN
  historial_compras AS t5
ON
  t1.id_cliente =t5.id_cliente
INNER JOIN
  historial_zonas AS t6
ON
  t1.id_cliente =t6.id_cliente
INNER JOIN
  historial_compras_periodo AS t7
ON
  t1.id_cliente =t7.id_cliente