CREATE OR REPLACE TABLE `customer-experience-384423.Data_refinada.Prd_estado_clientes_contactables` AS
SELECT
  t1.estado_email_venta,
  t1.estado_celular_venta,
  t1.habeas_data_venta,
  COUNT(DISTINCT(t1.id_cliente)) total_clientes
FROM
  `customer-experience-384423.Data_cruda.mineria_venta` AS t1
WHERE
  t1.id_cliente NOT IN (
    SELECT id
    FROM `customer-experience-384423.Data_cruda.blacklist`
  )
GROUP BY
  1,
  2,
  3
HAVING
  t1.habeas_data_venta = 'SI' AND (t1.estado_celular_venta = 'SI' OR t1.estado_celular_venta = 'SI');