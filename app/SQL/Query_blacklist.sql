CREATE OR REPLACE TABLE
  `customer-experience-384423.Data_refinada.prd_blacklist` AS
SELECT
  CAST(id AS int64) id,
  CAST(id_empresa AS int64) id_empresa,
  id_tipo_documento,
  id_usuario_agrega,
  numero_documento,
  nombre_1,
  nombre_2,
  apellido_1,
  apellido_2,
  fecha_nacimiento,
  numero_celular,
  numero_telefono,
  correo,
  genero,
  ciudad,
  estado,
  fecha_creado,
  fecha_editado,
  origen
FROM
  `customer-experience-384423.Data_cruda.blacklist`
