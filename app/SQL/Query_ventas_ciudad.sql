CREATE OR REPLACE TABLE  `customer-experience-384423.Data_refinada.prd_ventas_zona_ciudad` AS
SELECT
  t1.zona,
  t1.ciudad_pdv,
  SUM(t2.precio_antes_iva) total_ventas,
  COUNT(t2.id_factura) cantidad_facturas,
FROM
  `customer-experience-384423.Data_refinada.prd_mineria_almacen` AS t1
INNER JOIN
  `customer-experience-384423.Data_refinada.prd_mineria_ventas` AS t2
ON
  t1.id_pvd=t2.id_pdv
GROUP BY
  1,
  2
ORDER BY
  3 desc