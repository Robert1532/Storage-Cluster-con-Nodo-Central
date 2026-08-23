-- ============================================================================
--  Practica 1 - Storage Cluster CNS
--  Esquema MySQL 8.0  ·  Responsable: Robert (Datos)
--
--  Este archivo crea SOLO las tablas y las vistas. Funciona igual en Aiven y
--  en un MySQL local, porque no crea la base ni el usuario.
--
--  AIVEN (desarrollo, dias 1-5):
--      mysql --host=storagecluster-robertdenilo2005-eebb.a.aivencloud.com \
--            --port=20229 --user=avnadmin --password \
--            --ssl-ca=db/ca.pem defaultdb < db/schema.sql
--
--  LOCAL (demo, desde el dia 6): primero el preambulo, despues este archivo
--      mysql -u root -p < db/schema_local.sql
--      mysql -u root -p cns_cluster < db/schema.sql
--
--  Es idempotente: se puede volver a correr sin romper nada.
-- ============================================================================

DROP VIEW  IF EXISTS v_cluster;
DROP VIEW  IF EXISTS v_nodos_estado;
DROP VIEW  IF EXISTS v_ultima_metrica;
DROP TABLE IF EXISTS mensajes;
DROP TABLE IF EXISTS eventos;
DROP TABLE IF EXISTS metricas;
DROP TABLE IF EXISTS nodos;

-- ----------------------------------------------------------------------------
-- 1. nodos  ·  catalogo: quien existe en el cluster
--    Una fila por servidor regional. Aqui vive el ESTADO ACTUAL.
-- ----------------------------------------------------------------------------
CREATE TABLE nodos (
  id                 INT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id            VARCHAR(32)  NOT NULL,
  region             VARCHAR(64)  NOT NULL,
  hostname           VARCHAR(128) NULL,
  sistema_operativo  VARCHAR(64)  NULL,
  ip                 VARCHAR(45)  NULL,              -- 45 = cabe IPv6
  primer_registro    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  ultimo_reporte     DATETIME(3)  NULL,
  estado             ENUM('ACTIVO','NO_REPORTA') NOT NULL DEFAULT 'ACTIVO',
  intervalo_seg      SMALLINT UNSIGNED NOT NULL DEFAULT 10,
  PRIMARY KEY (id),
  UNIQUE KEY uq_node_id (node_id),                   -- el alta automatica depende de esto
  KEY ix_estado (estado),
  KEY ix_ultimo_reporte (ultimo_reporte)             -- lo lee el watchdog
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- primer_registro documenta CUANDO se dio de alta solo un nodo: es la
-- evidencia del requisito 7.2 que pueden mostrar en la defensa.


-- ----------------------------------------------------------------------------
-- 2. metricas  ·  historico: una fila por reporte, nunca se sobrescribe
--    9 nodos x 1 reporte cada 10 s = ~78.000 filas/dia. Por eso el indice.
-- ----------------------------------------------------------------------------
CREATE TABLE metricas (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id         VARCHAR(32)  NOT NULL,
  timestamp       DATETIME(3)  NOT NULL,
  disco_nombre    VARCHAR(64)  NULL,
  disco_tipo      ENUM('SSD','HDD','DESCONOCIDO') NOT NULL DEFAULT 'DESCONOCIDO',
  total_gb        DECIMAL(10,2) NOT NULL,
  usado_gb        DECIMAL(10,2) NOT NULL,
  libre_gb        DECIMAL(10,2) NOT NULL,
  uso_pct         DECIMAL(5,2)  NOT NULL,
  iops_lectura    INT UNSIGNED  NOT NULL DEFAULT 0,
  iops_escritura  INT UNSIGNED  NOT NULL DEFAULT 0,
  latencia_ms     DECIMAL(8,3)  NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY ix_nodo_tiempo (node_id, timestamp DESC),      -- indice compuesto: sirve
                                                     -- para "ultima de cada nodo"
                                                     -- y para la serie temporal
  CONSTRAINT fk_metricas_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Por que DECIMAL y no FLOAT: sumar nueve FLOAT da cosas como 4291.999999998 y
-- el % de utilizacion global sale con basura decimal. DECIMAL suma exacto.
-- Es una buena respuesta si preguntan por el diseno de tipos.


-- ----------------------------------------------------------------------------
-- 3. eventos  ·  bitacora: que le paso a cada nodo y cuando
--    De aqui salen los failover events y la disponibilidad.
-- ----------------------------------------------------------------------------
CREATE TABLE eventos (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id    VARCHAR(32) NOT NULL,
  timestamp  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  tipo       ENUM('CONEXION','DESCONEXION','ALTA_AUTOMATICA',
                  'NO_REPORTA','RECUPERADO','CAMBIO_INTERVALO') NOT NULL,
  detalle    VARCHAR(255) NULL,
  PRIMARY KEY (id),
  KEY ix_nodo_tiempo (node_id, timestamp DESC),
  KEY ix_tipo (tipo),
  CONSTRAINT fk_eventos_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- ----------------------------------------------------------------------------
-- 4. mensajes  ·  canal de vuelta servidor -> cliente, con su confirmacion
--
--    OJO, ESTA TABLA ES TAMBIEN EL BUS ENTRE LA API Y EL SERVIDOR DE SOCKETS.
--    La API (FastAPI) y el servidor de sockets son DOS PROCESOS distintos y no
--    comparten memoria. Cuando el dashboard manda un mensaje, la API inserta
--    aqui una fila con estado='PENDIENTE'; el despachador del servidor de
--    sockets consulta cada segundo las pendientes, las envia por el socket y
--    las pasa a 'ENVIADO'. Cuando llega el ACK, quedan en 'CONFIRMADO'.
--
--    Ciclo:  PENDIENTE -> ENVIADO -> CONFIRMADO   (o FALLIDO si el nodo no esta)
-- ----------------------------------------------------------------------------
CREATE TABLE mensajes (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  cmd_id      VARCHAR(36) NOT NULL,                  -- UUID: empareja el ACK
  node_id     VARCHAR(32) NOT NULL,
  accion      ENUM('MENSAJE','SET_INTERVAL') NOT NULL DEFAULT 'MENSAJE',
  texto       VARCHAR(255) NULL,
  valor       INT NULL,                              -- para SET_INTERVAL
  estado      ENUM('PENDIENTE','ENVIADO','CONFIRMADO','FALLIDO')
                 NOT NULL DEFAULT 'PENDIENTE',
  creado_en   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  enviado_en  DATETIME(3) NULL,
  ack_en      DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_cmd_id (cmd_id),
  KEY ix_pendientes (estado, creado_en),             -- lo lee el despachador
  KEY ix_nodo (node_id, creado_en DESC),
  CONSTRAINT fk_mensajes_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- La diferencia entre ack_en y enviado_en es el round-trip real del mensaje.
-- Es un dato lindo para mostrar en el dashboard y para la defensa.


-- ============================================================================
--  VISTAS
--  Sacan la logica repetida de Python y hacen la API mucho mas corta.
-- ============================================================================

-- Ultima metrica de CADA nodo.
-- Se usa ROW_NUMBER() y no "ORDER BY timestamp LIMIT 1" porque eso ultimo
-- devuelve una sola fila del cluster entero, no una por nodo. Es el error mas
-- comun de esta practica.
--
-- REQUIERE MySQL 8.0.14 O SUPERIOR: antes de esa version no se permitian
-- subconsultas en el FROM de una vista. Verifiquen con  SELECT VERSION();
-- Si les toca una version anterior, la alternativa es hacer el ROW_NUMBER()
-- directamente en la consulta de repositorio.py en vez de en la vista.
CREATE OR REPLACE VIEW v_ultima_metrica AS
SELECT id, node_id, timestamp, disco_nombre, disco_tipo,
       total_gb, usado_gb, libre_gb, uso_pct,
       iops_lectura, iops_escritura, latencia_ms
FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY m.node_id
                              ORDER BY m.timestamp DESC, m.id DESC) AS rn
    FROM metricas m
) AS t
WHERE rn = 1;


-- Estado completo de cada nodo: catalogo + su ultima medicion.
-- Es lo que consume GET /api/nodes casi tal cual.
CREATE OR REPLACE VIEW v_nodos_estado AS
SELECT
    n.node_id,
    n.region,
    n.hostname,
    n.sistema_operativo,
    n.ip,
    n.estado,
    n.intervalo_seg,
    n.primer_registro,
    n.ultimo_reporte,
    um.disco_nombre,
    um.disco_tipo,
    um.total_gb,
    um.usado_gb,
    um.libre_gb,
    um.uso_pct,
    um.iops_lectura,
    um.iops_escritura,
    um.latencia_ms,
    TIMESTAMPDIFF(SECOND, n.ultimo_reporte, NOW()) AS segundos_sin_reportar,
    (SELECT COUNT(*) FROM eventos e
      WHERE e.node_id = n.node_id AND e.tipo = 'NO_REPORTA') AS failover_events
FROM nodos n
LEFT JOIN v_ultima_metrica um ON um.node_id = n.node_id;


-- Consolidado del cluster: una sola fila con todos los totales.
-- Solo suma nodos ACTIVOS: un nodo caido no aporta capacidad disponible.
CREATE OR REPLACE VIEW v_cluster AS
SELECT
    (SELECT COUNT(*) FROM nodos)                              AS nodos_totales,
    (SELECT COUNT(*) FROM nodos WHERE estado = 'ACTIVO')      AS nodos_activos,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) AS capacidad_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) AS usado_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN libre_gb END), 0) AS libre_total_gb,
    ROUND(
      COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) /
      NULLIF(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) * 100
    , 2)                                                      AS uso_pct_global,
    ROUND(
      COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN latencia_ms * total_gb END), 0) /
      NULLIF(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0)
    , 3)                                                      AS latencia_ponderada_ms
FROM v_nodos_estado;

-- NULLIF evita la division por cero cuando todavia no reporto nadie.
-- Si les preguntan "que pasa si arrancan con cero nodos", la respuesta esta aca.
