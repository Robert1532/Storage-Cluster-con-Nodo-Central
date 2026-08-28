-- ============================================================================
--  Practica 1 - Storage Cluster CNS
--  MIGRACION v1 -> v2   ·  Responsable: Robert (Datos)
--
--  Este script agrega TODO lo nuevo de la version 2 SIN BORRAR NADA. Uselo si
--  la base ya tiene metricas que no quieren perder. Si no les importan los
--  datos, es mas simple correr db/schema.sql, que reconstruye todo.
--
--      mysql -u root -p cns_cluster < db/migracion_v2.sql
--      # Aiven:
--      mysql --host=... --port=... --user=avnadmin --password \
--            --ssl-ca=db/ca.pem defaultdb < db/migracion_v2.sql
--
--  Es IDEMPOTENTE: se puede correr dos veces sin romper nada. MySQL no tiene
--  "ADD COLUMN IF NOT EXISTS", asi que cada ALTER va envuelto en un
--  procedimiento que primero consulta information_schema. Es mas largo de leer
--  y evita tener que adivinar en que estado quedo la base de cada uno.
-- ============================================================================

SET time_zone = '+00:00';

DROP PROCEDURE IF EXISTS agregar_columna;
DELIMITER //
CREATE PROCEDURE agregar_columna(
    IN p_tabla   VARCHAR(64),
    IN p_columna VARCHAR(64),
    IN p_defin   TEXT)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME   = p_tabla
                      AND COLUMN_NAME  = p_columna) THEN
        SET @sql = CONCAT('ALTER TABLE `', p_tabla, '` ADD COLUMN `',
                          p_columna, '` ', p_defin);
        PREPARE st FROM @sql; EXECUTE st; DEALLOCATE PREPARE st;
    END IF;
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS agregar_indice;
DELIMITER //
CREATE PROCEDURE agregar_indice(
    IN p_tabla   VARCHAR(64),
    IN p_indice  VARCHAR(64),
    IN p_cols    TEXT)
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME   = p_tabla
                      AND INDEX_NAME   = p_indice) THEN
        SET @sql = CONCAT('ALTER TABLE `', p_tabla, '` ADD KEY `',
                          p_indice, '` (', p_cols, ')');
        PREPARE st FROM @sql; EXECUTE st; DEALLOCATE PREPARE st;
    END IF;
END //
DELIMITER ;


-- ---------------------------------------------------------------- 1. nodos --
CALL agregar_columna('nodos','sede',              'VARCHAR(64) NULL');
CALL agregar_columna('nodos','agente_version',    'VARCHAR(32) NULL');
CALL agregar_columna('nodos','capacidades',       'VARCHAR(255) NULL');
CALL agregar_columna('nodos','recursos_pedidos',  'VARCHAR(255) NULL');
CALL agregar_columna('nodos','ultima_desconexion','DATETIME(3) NULL');
CALL agregar_columna('nodos','motivo_desconexion','VARCHAR(255) NULL');
CALL agregar_columna('nodos','ultima_reconexion', 'DATETIME(3) NULL');
CALL agregar_columna('nodos','intermitente',      'TINYINT(1) NOT NULL DEFAULT 0');
CALL agregar_columna('nodos','caidas_recientes',  'SMALLINT UNSIGNED NOT NULL DEFAULT 0');
CALL agregar_columna('nodos','ultima_seq',        'BIGINT UNSIGNED NOT NULL DEFAULT 0');
CALL agregar_columna('nodos','pendientes_sync',   'INT UNSIGNED NOT NULL DEFAULT 0');
CALL agregar_columna('nodos','desvio_reloj_seg',  'DECIMAL(12,3) NULL');
CALL agregar_indice ('nodos','ix_region',         '`region`');


-- ------------------------------------------------------------- 2. metricas --
CALL agregar_columna('metricas','seq',      'BIGINT UNSIGNED NOT NULL DEFAULT 0');
CALL agregar_columna('metricas','origen',   "ENUM('VIVO','SYNC') NOT NULL DEFAULT 'VIVO'");
CALL agregar_columna('metricas','t_cliente','DATETIME(3) NULL');
CALL agregar_indice ('metricas','ix_nodo_seq','`node_id`,`seq`');

-- USB es un tipo de disco nuevo: el pendrive que se enchufa en la laptop.
ALTER TABLE metricas
  MODIFY COLUMN disco_tipo ENUM('SSD','HDD','USB','DESCONOCIDO')
    NOT NULL DEFAULT 'DESCONOCIDO';


-- --------------------------------------------------------- 3. eventos ------
ALTER TABLE eventos
  MODIFY COLUMN tipo ENUM('CONEXION','DESCONEXION','ALTA_AUTOMATICA',
                          'NO_REPORTA','RECUPERADO','CAMBIO_INTERVALO',
                          'SINCRONIZACION','RELOJ_DESVIADO','INTERMITENTE',
                          'DISCO_AGREGADO','DISCO_REMOVIDO',
                          'CAPACIDAD_CAMBIADA','CAMBIO_RECURSOS') NOT NULL;


-- -------------------------------------------------------- 4. mensajes ------
ALTER TABLE mensajes
  MODIFY COLUMN accion ENUM('MENSAJE','SET_INTERVAL',
                            'SET_RECURSOS','PING','SOLICITAR_SYNC')
    NOT NULL DEFAULT 'MENSAJE';


-- -------------------------------------------------------- 5. recursos ------
-- La tabla flexible. Ver el comentario largo en db/schema.sql.
CREATE TABLE IF NOT EXISTS recursos (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id    VARCHAR(32) NOT NULL,
  timestamp  DATETIME(3) NOT NULL,
  tipo       ENUM('DISCO','RAM','CPU','RED','CUSTOM') NOT NULL DEFAULT 'CUSTOM',
  nombre     VARCHAR(64) NOT NULL,
  metricas   JSON NOT NULL,
  etiquetas  JSON NULL,
  origen     ENUM('VIVO','SYNC') NOT NULL DEFAULT 'VIVO',
  seq        BIGINT UNSIGNED NOT NULL DEFAULT 0,
  total_gb   DECIMAL(12,2) GENERATED ALWAYS AS
             (JSON_VALUE(metricas, '$.total_gb' RETURNING DECIMAL(12,2) NULL ON EMPTY NULL ON ERROR)) STORED,
  usado_gb   DECIMAL(12,2) GENERATED ALWAYS AS
             (JSON_VALUE(metricas, '$.usado_gb' RETURNING DECIMAL(12,2) NULL ON EMPTY NULL ON ERROR)) STORED,
  uso_pct    DECIMAL(6,2)  GENERATED ALWAYS AS
             (JSON_VALUE(metricas, '$.uso_pct' RETURNING DECIMAL(6,2) NULL ON EMPTY NULL ON ERROR)) STORED,
  PRIMARY KEY (id),
  KEY ix_nodo_recurso_tiempo (node_id, tipo, nombre, timestamp DESC, id DESC),
  KEY ix_tiempo (timestamp),
  KEY ix_uso (tipo, uso_pct),
  CONSTRAINT fk_recursos_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ------------------------------------------------------------- 6. vistas ---
-- Las vistas se recrean enteras: CREATE OR REPLACE las deja como en schema.sql.
-- El orden importa, porque v_nodos_estado usa v_recursos_ultimo.
CREATE OR REPLACE VIEW v_ultima_metrica AS
SELECT m.id, m.node_id, m.timestamp, m.disco_nombre, m.disco_tipo,
       m.total_gb, m.usado_gb, m.libre_gb, m.uso_pct,
       m.iops_lectura, m.iops_escritura, m.latencia_ms,
       m.origen, m.seq, m.t_cliente
FROM nodos n
JOIN metricas m
  ON m.id = (SELECT x.id
               FROM metricas x FORCE INDEX (ix_nodo_tiempo)
              WHERE x.node_id = n.node_id
              ORDER BY x.timestamp DESC, x.id DESC
              LIMIT 1);

CREATE OR REPLACE VIEW v_recursos_ultimo AS
SELECT r.id, r.node_id, r.tipo, r.nombre, r.timestamp,
       r.metricas, r.etiquetas, r.origen,
       r.total_gb, r.usado_gb, r.uso_pct
FROM recursos r
WHERE r.id = (SELECT x.id
                FROM recursos x
               WHERE x.node_id = r.node_id
                 AND x.tipo    = r.tipo
                 AND x.nombre  = r.nombre
               ORDER BY x.timestamp DESC, x.id DESC
               LIMIT 1);

CREATE OR REPLACE VIEW v_nodos_estado AS
SELECT
    n.node_id, n.region, n.sede, n.hostname, n.sistema_operativo, n.ip,
    n.estado, n.intervalo_seg, n.primer_registro, n.ultimo_reporte,
    n.agente_version, n.capacidades, n.recursos_pedidos,
    n.ultima_desconexion, n.motivo_desconexion, n.ultima_reconexion,
    n.intermitente, n.caidas_recientes, n.ultima_seq, n.pendientes_sync,
    n.desvio_reloj_seg,
    um.disco_nombre, um.disco_tipo, um.total_gb, um.usado_gb, um.libre_gb,
    um.uso_pct, um.iops_lectura, um.iops_escritura, um.latencia_ms,
    um.origen AS origen_ultima_metrica,
    TIMESTAMPDIFF(SECOND, n.ultimo_reporte, NOW()) AS segundos_sin_reportar,
    (SELECT COUNT(*) FROM eventos e
      WHERE e.node_id = n.node_id AND e.tipo = 'NO_REPORTA') AS failover_events,
    (SELECT COUNT(*) FROM v_recursos_ultimo vr
      WHERE vr.node_id = n.node_id)                          AS recursos_activos,
    COALESCE((SELECT SUM(vr.total_gb) FROM v_recursos_ultimo vr
               WHERE vr.node_id = n.node_id AND vr.tipo = 'DISCO'), 0)
                                                             AS extra_disco_gb
FROM nodos n
LEFT JOIN v_ultima_metrica um ON um.node_id = n.node_id;

CREATE OR REPLACE VIEW v_cluster AS
SELECT
    (SELECT COUNT(*) FROM nodos)                              AS nodos_totales,
    (SELECT COUNT(*) FROM nodos WHERE estado = 'ACTIVO')      AS nodos_activos,
    (SELECT COUNT(*) FROM nodos WHERE intermitente = 1)       AS nodos_intermitentes,
    (SELECT COUNT(DISTINCT region) FROM nodos)                AS regionales,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) AS capacidad_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) AS usado_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN libre_gb END), 0) AS libre_total_gb,
    ROUND(COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) /
          NULLIF(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) * 100, 2)
                                                              AS uso_pct_global,
    ROUND(COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN latencia_ms * total_gb END), 0) /
          NULLIF(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0), 3)
                                                              AS latencia_ponderada_ms,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN total_gb + extra_disco_gb END), 0)
                                                              AS capacidad_con_extras_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN extra_disco_gb END), 0)
                                                              AS extras_gb
FROM v_nodos_estado;

CREATE OR REPLACE VIEW v_regionales AS
SELECT
    region,
    COUNT(*)                                                  AS nodos,
    GROUP_CONCAT(sede ORDER BY sede SEPARATOR ' · ')          AS sedes,
    SUM(estado = 'ACTIVO')                                    AS nodos_activos,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) AS capacidad_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) AS usado_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN libre_gb END), 0) AS libre_total_gb,
    ROUND(COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) /
          NULLIF(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) * 100, 2) AS uso_pct,
    MAX(ultimo_reporte)                                       AS ultimo_reporte
FROM v_nodos_estado
GROUP BY region;


-- Limpieza: los procedimientos auxiliares no tienen por que quedarse.
DROP PROCEDURE IF EXISTS agregar_columna;
DROP PROCEDURE IF EXISTS agregar_indice;

SELECT 'Migracion v2 aplicada' AS resultado;
