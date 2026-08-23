-- ============================================================================
--  Preambulo SOLO para MySQL local (desde el dia 6, para la demo).
--  En Aiven NO se corre: la base ya existe (defaultdb) y avnadmin no tiene
--  permiso para crear bases ni usuarios.
--
--      mysql -u root -p < db/schema_local.sql
--      mysql -u root -p cns_cluster < db/schema.sql
-- ============================================================================

DROP DATABASE IF EXISTS cns_cluster;
CREATE DATABASE cns_cluster
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

-- Usuario de aplicacion. No usar root desde el codigo: si preguntan por
-- seguridad en la defensa, esto es un punto a favor.
CREATE USER IF NOT EXISTS 'cns_app'@'%' IDENTIFIED BY 'CAMBIAR_ESTA_CLAVE';
GRANT SELECT, INSERT, UPDATE, DELETE ON cns_cluster.* TO 'cns_app'@'%';
FLUSH PRIVILEGES;
