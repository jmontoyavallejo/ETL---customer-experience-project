CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.Prd_clientes_simplificado` AS
WITH
  promedio AS (
  SELECT
    id_cliente,
    AVG(ticket ) promedio_compra,
    count(*) cantidad_compras,
    AVG(ticket )/count(*) dropsize
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
    id_cliente),historial_compras as(
      SELECT
  t1.id_cliente,
  MAX(CASE      WHEN t2.tipo_de_prenda = '3 ELEFANTES' THEN 1    ELSE    0  END    ) AS compro_objeto_3_ELEFANTES,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'PELUCHES' THEN 1    ELSE    0  END    ) AS compro_objeto_PELUCHES,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'CALCETINES' THEN 1    ELSE    0  END    ) AS compro_objeto_CALCETINES,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'PELUCHE' THEN 1    ELSE    0  END    ) AS compro_objeto_PELUCHE,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BOLSO' THEN 1    ELSE    0  END    ) AS compro_objeto_BOLSO,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'CARTUCHERA' THEN 1    ELSE    0  END    ) AS compro_objeto_CARTUCHERA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BILLETERA' THEN 1    ELSE    0  END    ) AS compro_objeto_BILLETERA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'FUNKOS' THEN 1    ELSE    0  END    ) AS compro_objeto_FUNKOS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SALIDA DE BAÑO' THEN 1    ELSE    0  END    ) AS compro_objeto_SALIDA_DE_BANO,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'GAFAS' THEN 1    ELSE    0  END    ) AS compro_objeto_GAFAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SET DE ALIMENTACIÑN' THEN 1    ELSE    0  END    ) AS compro_objeto_SET_DE_ALIMENTACION,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'JUGUETERIA' THEN 1    ELSE    0  END    ) AS compro_objeto_JUGUETERIA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'CARTERA' THEN 1    ELSE    0  END    ) AS compro_objeto_CARTERA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'MALETA VIAJERA' THEN 1    ELSE    0  END    ) AS compro_objeto_MALETA_VIAJERA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'TENIS CON RUEDAS' THEN 1    ELSE    0  END    ) AS compro_objeto_TENIS_CON_RUEDAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SET DE BELLEZA' THEN 1    ELSE    0  END    ) AS compro_objeto_SET_DE_BELLEZA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SUECOS' THEN 1    ELSE    0  END    ) AS compro_objeto_SUECOS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'MORRAL' THEN 1    ELSE    0  END    ) AS compro_objeto_MORRAL,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'PANTUFLAS' THEN 1    ELSE    0  END    ) AS compro_objeto_PANTUFLAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'PARAGUAS' THEN 1    ELSE    0  END    ) AS compro_objeto_PARAGUAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'LONCHERA' THEN 1    ELSE    0  END    ) AS compro_objeto_LONCHERA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SANDALIAS' THEN 1    ELSE    0  END    ) AS compro_objeto_SANDALIAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BOTAS' THEN 1    ELSE    0  END    ) AS compro_objeto_BOTAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BONO' THEN 1    ELSE    0  END    ) AS compro_objeto_BONO,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'MALETA CON RUEDAS' THEN 1    ELSE    0  END    ) AS compro_objeto_MALETA_CON_RUEDAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SET ESCOLAR' THEN 1    ELSE    0  END    ) AS compro_objeto_SET_ESCOLAR,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'GORRAS' THEN 1    ELSE    0  END    ) AS compro_objeto_GORRAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'GORRO' THEN 1    ELSE    0  END    ) AS compro_objeto_GORRO,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'COBIJA' THEN 1    ELSE    0  END    ) AS compro_objeto_COBIJA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BABERO' THEN 1    ELSE    0  END    ) AS compro_objeto_BABERO,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'TENIS' THEN 1    ELSE    0  END    ) AS compro_objeto_TENIS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BALETA' THEN 1    ELSE    0  END    ) AS compro_objeto_BALETA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'ZAPATOS' THEN 1    ELSE    0  END    ) AS compro_objeto_ZAPATOS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'TENIS CON LUCES' THEN 1    ELSE    0  END    ) AS compro_objeto_TENIS_CON_LUCES,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'CORREAS' THEN 1    ELSE    0  END    ) AS compro_objeto_CORREAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'AROMAS' THEN 1    ELSE    0  END    ) AS compro_objeto_AROMAS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'POP' THEN 1    ELSE    0  END    ) AS compro_objeto_POP,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'BOLSA' THEN 1    ELSE    0  END    ) AS compro_objeto_BOLSA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'RELOJ' THEN 1    ELSE    0  END    ) AS compro_objeto_RELOJ,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'MOCHILA' THEN 1    ELSE    0  END    ) AS compro_objeto_MOCHILA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'ESTUCHES' THEN 1    ELSE    0  END    ) AS compro_objeto_ESTUCHES,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'PAÑALERA' THEN 1    ELSE    0  END    ) AS compro_objeto_PANALERA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'DULCES' THEN 1    ELSE    0  END    ) AS compro_objeto_DULCES,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'CAMISETA' THEN 1    ELSE    0  END    ) AS compro_objeto_CAMISETA,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'SET' THEN 1    ELSE    0  END    ) AS compro_objeto_SET,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'STICKERS' THEN 1    ELSE    0  END    ) AS compro_objeto_STICKERS,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'PANTALON' THEN 1    ELSE    0  END    ) AS compro_objeto_PANTALON,
  MAX(CASE      WHEN t2.tipo_de_prenda = 'ACCESORIOS' THEN 1    ELSE    0  END    ) AS compro_objeto_ACCESORIOS
FROM
  `customer-experience-384423.Data_refinada.prd_mineria_ventas` t1
LEFT JOIN
  `customer-experience-384423.Data_refinada.prd_mineria_productos` t2
ON
  t1.id_referencia = t2.id_referencia
GROUP BY
  t1.id_cliente
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
    t4.dias_desde_ultima_compra,
  t5.compro_objeto_3_ELEFANTES,
  t5.compro_objeto_PELUCHES,
  t5.compro_objeto_CALCETINES,
  t5.compro_objeto_PELUCHE,
  t5.compro_objeto_BOLSO,
  t5.compro_objeto_CARTUCHERA,
  t5.compro_objeto_BILLETERA,
  t5.compro_objeto_FUNKOS,
  t5.compro_objeto_SALIDA_DE_BANO,
  t5.compro_objeto_GAFAS,
  t5.compro_objeto_SET_DE_ALIMENTACION,
  t5.compro_objeto_JUGUETERIA,
  t5.compro_objeto_CARTERA,
  t5.compro_objeto_MALETA_VIAJERA,
  t5.compro_objeto_TENIS_CON_RUEDAS,
  t5.compro_objeto_SET_DE_BELLEZA,
  t5.compro_objeto_SUECOS,
  t5.compro_objeto_MORRAL,
  t5.compro_objeto_PANTUFLAS,
  t5.compro_objeto_PARAGUAS,
  t5.compro_objeto_LONCHERA,
  t5.compro_objeto_SANDALIAS,
  t5.compro_objeto_BOTAS,
  t5.compro_objeto_BONO,
  t5.compro_objeto_MALETA_CON_RUEDAS,
  t5.compro_objeto_SET_ESCOLAR,
  t5.compro_objeto_GORRAS,
  t5.compro_objeto_GORRO,
  t5.compro_objeto_COBIJA,
  t5.compro_objeto_BABERO,
  t5.compro_objeto_TENIS,
  t5.compro_objeto_BALETA,
  t5.compro_objeto_ZAPATOS,
  t5.compro_objeto_TENIS_CON_LUCES,
  t5.compro_objeto_CORREAS,
  t5.compro_objeto_AROMAS,
  t5.compro_objeto_POP,
  t5.compro_objeto_BOLSA,
  t5.compro_objeto_RELOJ,
  t5.compro_objeto_MOCHILA,
  t5.compro_objeto_ESTUCHES,
  t5.compro_objeto_PANALERA,
  t5.compro_objeto_DULCES,
  t5.compro_objeto_CAMISETA,
  t5.compro_objeto_SET,
  t5.compro_objeto_STICKERS,
  t5.compro_objeto_PANTALON,
  t5.compro_objeto_ACCESORIOS,
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
