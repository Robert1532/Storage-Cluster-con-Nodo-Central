-- ============================================================================
--
--   STORAGE CLUSTER CNS  ·  SCRIPT COMPLETO DE LA BASE DE DATOS  ·  v2 FINAL
--   Practica 1 - Implementacion de Sockets
--   MySQL 8.0
--
--   ESTE ES EL UNICO ARCHIVO QUE HAY QUE CORRER PARA DEJAR LA BASE LISTA.
--   Crea la base, el usuario de aplicacion, las 5 tablas y las 5 vistas.
--
--   -------------------------------------------------------------------------
--   COMO SE CORRE
--   -------------------------------------------------------------------------
--
--   1. Cambiar la clave de abajo (buscar CAMBIAR_ESTA_CLAVE, esta dos veces)
--      y poner LA MISMA en DB_PASSWORD del archivo .env
--
--   2. Windows / Linux / macOS:
--
--          mysql -u root -p < db/instalar.sql
--
--   3. Comprobar que quedo bien:
--
--          python -m db.probar_aiven      # estructura
--          python -m db.probar_bd         # capa de datos
--
--   -------------------------------------------------------------------------
--   ATENCION
--   -------------------------------------------------------------------------
--
--   Este script EMPIEZA BORRANDO la base cns_cluster entera. Se puede correr
--   cuantas veces haga falta, pero se lleva por delante todos los datos.
--
--   * Si ya tienen datos de la version 1 y NO los quieren perder:
--     no corran este archivo — corran db/migracion_v2.sql, que agrega lo nuevo
--     sin borrar nada.
--
--   * Contra Aiven (la base compartida del equipo) NO se corre este archivo:
--     avnadmin no puede crear bases ni usuarios, y ademas borraria el trabajo
--     de los otros cuatro. Ahi va solamente db/schema.sql, sobre defaultdb.
--
--   -------------------------------------------------------------------------
--   QUE QUEDA CREADO
--   -------------------------------------------------------------------------
--
--   TABLAS
--     nodos      catalogo: quien existe en el cluster y su estado actual
--     metricas   historico del PRIMER disco (lo que pide el enunciado)
--     recursos   TODO lo demas: RAM, CPU, red, discos adicionales (JSON)
--     eventos    bitacora: altas, caidas, recuperaciones, sincronizaciones
--     mensajes   canal servidor -> cliente, y bus entre la API y los sockets
--
--   VISTAS
--     v_ultima_metrica    la ultima medicion de cada nodo
--     v_recursos_ultimo   la ultima medicion de cada recurso de cada nodo
--     v_nodos_estado      catalogo + ultima medicion  (lo consume GET /api/nodes)
--     v_cluster           una fila con los totales del cluster
--     v_regionales        totales POR DEPARTAMENTO (La Paz suma sus dos sedes)
--
--   Responsable: Robert (Datos y Coordinacion)
-- ============================================================================


-- ============================================================================
--  PARTE 1 de 3  ·  LA BASE Y EL USUARIO
-- ============================================================================

DROP DATABASE IF EXISTS cns_cluster;

CREATE DATABASE cns_cluster
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_0900_ai_ci;

-- utf8mb4 y no utf8: el utf8 de MySQL es de tres bytes y no cubre el rango
-- completo de Unicode. Un nombre de disco o un mensaje con un caracter de
-- cuatro bytes (cualquier emoji, por ejemplo) daria error de insercion en
-- modo estricto. utf8mb4 es el UTF-8 de verdad.

-- Usuario de aplicacion. El codigo NUNCA se conecta como root: si en la
-- defensa preguntan por seguridad, esto es un punto a favor — una inyeccion
-- SQL con este usuario no puede borrar la base ni crear usuarios, porque no
-- tiene DROP ni CREATE.
--
-- El ALTER no sobra: CREATE USER IF NOT EXISTS no cambia la contrasena si el
-- usuario ya existia, y entonces el .env queda desincronizado y el error de
-- autenticacion no dice por que.
CREATE USER IF NOT EXISTS 'cns_app'@'%' IDENTIFIED BY 'CAMBIAR_ESTA_CLAVE';
ALTER  USER 'cns_app'@'%' IDENTIFIED BY 'CAMBIAR_ESTA_CLAVE';

GRANT SELECT, INSERT, UPDATE, DELETE ON cns_cluster.* TO 'cns_app'@'%';
FLUSH PRIVILEGES;

USE cns_cluster;

-- Todo el sistema trabaja en UTC. Esto importa tambien aqui: las columnas con
-- DEFAULT CURRENT_TIMESTAMP(3) se evaluan con la zona de la SESION que
-- inserta, asi que una fila metida a mano desde el cliente de MySQL en una
-- maquina en UTC-4 quedaria cuatro horas desplazada.
SET time_zone = '+00:00';


-- ============================================================================
--  PARTE 2 de 3  ·  TABLAS Y VISTAS
-- ============================================================================

DROP VIEW  IF EXISTS v_cluster;
DROP VIEW  IF EXISTS v_regionales;
DROP VIEW  IF EXISTS v_nodos_estado;
DROP VIEW  IF EXISTS v_ultima_metrica;
DROP VIEW  IF EXISTS v_recursos_ultimo;
DROP TABLE IF EXISTS mensajes;
DROP TABLE IF EXISTS eventos;
DROP TABLE IF EXISTS recursos;
DROP TABLE IF EXISTS metricas;
DROP TABLE IF EXISTS nodos;


-- ----------------------------------------------------------------------------
-- 1. nodos  ·  catalogo: quien existe en el cluster
--    Una fila por servidor regional. Aqui vive el ESTADO ACTUAL.
--
--    OJO: la REGION NO ES LA IDENTIDAD. La Paz tiene dos servidores
--    (CNS-LPZ-01 y CNS-LPZ-10) y las dos filas dicen region='La Paz'. Lo unico
--    unico es node_id. Por eso uq_node_id esta sobre node_id y NO sobre region.
-- ----------------------------------------------------------------------------
CREATE TABLE nodos (
  id                 INT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id            VARCHAR(32)  NOT NULL,
  -- region = DEPARTAMENTO (hay nueve: las administraciones regionales del
  -- enunciado). sede = la oficina concreta donde esta el servidor. El
  -- departamento de La Paz tiene dos sedes con servidor propio: La Paz y El
  -- Alto. Se agrupa por region; se distingue por sede.
  region             VARCHAR(64)  NOT NULL,
  sede               VARCHAR(64)  NULL,
  hostname           VARCHAR(128) NULL,
  sistema_operativo  VARCHAR(64)  NULL,
  ip                 VARCHAR(45)  NULL,              -- 45 = cabe IPv6
  primer_registro    DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  ultimo_reporte     DATETIME(3)  NULL,
  estado             ENUM('ACTIVO','NO_REPORTA') NOT NULL DEFAULT 'ACTIVO',
  intervalo_seg      SMALLINT UNSIGNED NOT NULL DEFAULT 10,

  -- ---------------------------------------------------------------- v2 ------
  agente_version     VARCHAR(32)  NULL,              -- version del cliente
  capacidades        VARCHAR(255) NULL,              -- que sabe medir el nodo
  recursos_pedidos   VARCHAR(255) NULL,              -- que le pide el servidor

  -- "Se desconecto de la red el ...". Antes esta informacion solo existia en
  -- la tabla eventos y el dashboard no la mostraba en ningun lado.
  ultima_desconexion DATETIME(3)  NULL,
  motivo_desconexion VARCHAR(255) NULL,
  ultima_reconexion  DATETIME(3)  NULL,

  -- Un nodo que se cae y vuelve cinco veces en diez minutos no esta "activo":
  -- esta fallando de forma intermitente, y es lo que hay que mirar primero.
  intermitente       TINYINT(1)   NOT NULL DEFAULT 0,
  caidas_recientes   SMALLINT UNSIGNED NOT NULL DEFAULT 0,

  -- Ultimo numero de muestra aceptado. Es lo que hace idempotente la
  -- sincronizacion: si el cliente reenvia un lote porque no le llego el
  -- SYNC_OK, todo lo que sea <= a esto se descarta en vez de duplicarse.
  ultima_seq         BIGINT UNSIGNED NOT NULL DEFAULT 0,
  pendientes_sync    INT UNSIGNED NOT NULL DEFAULT 0,

  -- Cuanto miente el reloj de ese nodo respecto al del servidor, en segundos.
  -- Informativo: la hora de las metricas la pone el servidor igual.
  desvio_reloj_seg   DECIMAL(12,3) NULL,

  PRIMARY KEY (id),
  UNIQUE KEY uq_node_id (node_id),                   -- el alta automatica depende de esto
  KEY ix_estado (estado),
  KEY ix_region (region),                            -- agrupacion por regional
  KEY ix_ultimo_reporte (ultimo_reporte),            -- lo lee el watchdog
  -- Un intervalo de 0 haria que el watchdog marque NO_REPORTA un segundo
  -- despues de cada reporte, para siempre.
  CONSTRAINT ck_intervalo CHECK (intervalo_seg BETWEEN 1 AND 3600)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- primer_registro documenta CUANDO se dio de alta solo un nodo: es la
-- evidencia del requisito 7.2 que pueden mostrar en la defensa.


-- ----------------------------------------------------------------------------
-- 2. metricas  ·  historico del PRIMER disco: una fila por reporte
--    Es la tabla que exige el enunciado ("solo se reporta el primer disco") y
--    la que alimenta v_cluster y el dashboard. Los discos ADICIONALES, la RAM
--    y la CPU van en `recursos`.
--
--    9 nodos x 1 reporte cada 10 s = ~78.000 filas/dia. Por eso los indices.
-- ----------------------------------------------------------------------------
CREATE TABLE metricas (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id         VARCHAR(32)  NOT NULL,
  timestamp       DATETIME(3)  NOT NULL,             -- LA PONE EL SERVIDOR
  disco_nombre    VARCHAR(64)  NULL,
  disco_tipo      ENUM('SSD','HDD','USB','DESCONOCIDO') NOT NULL DEFAULT 'DESCONOCIDO',
  total_gb        DECIMAL(10,2) NOT NULL,
  usado_gb        DECIMAL(10,2) NOT NULL,
  libre_gb        DECIMAL(10,2) NOT NULL,
  uso_pct         DECIMAL(5,2)  NOT NULL,
  iops_lectura    INT UNSIGNED  NOT NULL DEFAULT 0,
  iops_escritura  INT UNSIGNED  NOT NULL DEFAULT 0,
  latencia_ms     DECIMAL(8,3)  NOT NULL DEFAULT 0,

  -- ---------------------------------------------------------------- v2 ------
  -- Numero de muestra del cliente. Crece siempre y sobrevive a un reinicio
  -- del cliente porque vive en su base local SQLite.
  seq             BIGINT UNSIGNED NOT NULL DEFAULT 0,
  -- VIVO: llego en el momento.  SYNC: se recupero del buffer del cliente
  -- despues de una caida de red. En el dashboard se distinguen a proposito:
  -- un hueco relleno no es lo mismo que un dato de tiempo real.
  origen          ENUM('VIVO','SYNC') NOT NULL DEFAULT 'VIVO',
  -- Hora que decia el reloj DEL CLIENTE. Solo para auditoria: si no coincide
  -- con `timestamp`, ese nodo tiene la hora cambiada.
  t_cliente       DATETIME(3)  NULL,

  PRIMARY KEY (id),
  KEY ix_nodo_tiempo (node_id, timestamp DESC, id DESC),  -- ultima metrica de un
                                                     -- nodo (el id desempata) y
                                                     -- serie temporal por nodo
  KEY ix_tiempo (timestamp),                         -- consultas por ventana de
                                                     -- tiempo SIN filtrar nodo
                                                     -- (growth rate global)
  KEY ix_nodo_seq (node_id, seq),                    -- control de duplicados
  CONSTRAINT fk_metricas_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Por que DECIMAL y no FLOAT: sumar nueve FLOAT da cosas como 4291.999999998 y
-- el % de utilizacion global sale con basura decimal. DECIMAL suma exacto.
-- Es una buena respuesta si preguntan por el diseno de tipos.


-- ----------------------------------------------------------------------------
-- 3. recursos  ·  LA TABLA FLEXIBLE (v2)
--
--    Aqui entra cualquier cosa que un nodo sepa medir y que no sea el primer
--    disco: la RAM, la CPU, las interfaces de red, y los discos adicionales
--    (el pendrive que alguien enchufa en la laptop de Santa Cruz).
--
--    POR QUE JSON Y NO UNA COLUMNA POR METRICA
--    Porque el objetivo es que agregar una medida nueva NO obligue a un ALTER
--    TABLE ni a coordinar a cinco personas. El cliente manda
--    {"total_gb": 16, "usado_gb": 9.2, "uso_pct": 57.5} y se guarda entero.
--    Manana manda ademas {"temperatura_c": 41} y tambien se guarda.
--
--    POR QUE ADEMAS HAY COLUMNAS GENERADAS
--    Porque consultar JSON en cada fila es lento y no se indexa. Las tres
--    medidas que SI se consultan siempre (total, usado, %) se extraen a
--    columnas STORED calculadas por MySQL a partir del JSON: se escriben
--    solas, no se pueden desincronizar del JSON, y SI se indexan. Se paga
--    espacio y se gana orden de magnitud en las consultas del dashboard.
--    Es el patron "JSON con columnas materializadas" y es una buena respuesta
--    si en la defensa preguntan por que no se hizo una tabla clave-valor.
-- ----------------------------------------------------------------------------
CREATE TABLE recursos (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id    VARCHAR(32) NOT NULL,
  timestamp  DATETIME(3) NOT NULL,                   -- LA PONE EL SERVIDOR
  tipo       ENUM('DISCO','RAM','CPU','RED','CUSTOM') NOT NULL DEFAULT 'CUSTOM',
  nombre     VARCHAR(64) NOT NULL,                   -- 'C:\\', 'fisica', 'eth0'
  metricas   JSON NOT NULL,                          -- solo numeros
  etiquetas  JSON NULL,                              -- texto descriptivo
  origen     ENUM('VIVO','SYNC') NOT NULL DEFAULT 'VIVO',
  seq        BIGINT UNSIGNED NOT NULL DEFAULT 0,

  -- Columnas materializadas desde el JSON. NULL si ese recurso no reporta esa
  -- medida (la CPU no tiene total_gb, y esta bien que sea NULL).
  total_gb   DECIMAL(12,2) GENERATED ALWAYS AS
             (JSON_VALUE(metricas, '$.total_gb' RETURNING DECIMAL(12,2) NULL ON EMPTY NULL ON ERROR)) STORED,
  usado_gb   DECIMAL(12,2) GENERATED ALWAYS AS
             (JSON_VALUE(metricas, '$.usado_gb' RETURNING DECIMAL(12,2) NULL ON EMPTY NULL ON ERROR)) STORED,
  uso_pct    DECIMAL(6,2)  GENERATED ALWAYS AS
             (JSON_VALUE(metricas, '$.uso_pct' RETURNING DECIMAL(6,2) NULL ON EMPTY NULL ON ERROR)) STORED,

  PRIMARY KEY (id),
  -- El indice lleva el nombre del recurso porque la consulta tipica es
  -- "la ultima medicion de la RAM del nodo X", no "todas las de la RAM".
  KEY ix_nodo_recurso_tiempo (node_id, tipo, nombre, timestamp DESC, id DESC),
  KEY ix_tiempo (timestamp),
  KEY ix_uso (tipo, uso_pct),
  CONSTRAINT fk_recursos_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ----------------------------------------------------------------------------
-- 4. eventos  ·  bitacora: que le paso a cada nodo y cuando
--    De aqui salen los failover events y la disponibilidad.
-- ----------------------------------------------------------------------------
CREATE TABLE eventos (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  node_id    VARCHAR(32) NOT NULL,
  timestamp  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  tipo       ENUM('CONEXION','DESCONEXION','ALTA_AUTOMATICA',
                  'NO_REPORTA','RECUPERADO','CAMBIO_INTERVALO',
                  -- v2:
                  'SINCRONIZACION',     -- llego un lote de datos atrasados
                  'RELOJ_DESVIADO',     -- ese nodo tiene la hora cambiada
                  'INTERMITENTE',       -- se cae y vuelve una y otra vez
                  'DISCO_AGREGADO',     -- enchufaron un pendrive / disco nuevo
                  'DISCO_REMOVIDO',     -- lo sacaron
                  'CAPACIDAD_CAMBIADA', -- el disco crecio o encogio
                  'CAMBIO_RECURSOS'     -- se le pidio medir otra cosa
                 ) NOT NULL,
  detalle    VARCHAR(255) NULL,
  PRIMARY KEY (id),
  KEY ix_nodo_tiempo (node_id, timestamp DESC),
  KEY ix_tiempo (timestamp DESC),                    -- la bitacora global del
                                                     -- dashboard, sin filtro
  KEY ix_tipo (tipo),
  CONSTRAINT fk_eventos_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


-- ----------------------------------------------------------------------------
-- 5. mensajes  ·  canal de vuelta servidor -> cliente, con su confirmacion
--
--    OJO, ESTA TABLA ES TAMBIEN EL BUS ENTRE LA API Y EL SERVIDOR DE SOCKETS.
--    La API (FastAPI) y el servidor de sockets son DOS PROCESOS distintos y no
--    comparten memoria. Cuando el dashboard manda un mensaje, la API inserta
--    aqui una fila con estado='PENDIENTE'; el despachador del servidor de
--    sockets consulta cada segundo las pendientes, las envia por el socket y
--    las pasa a 'ENVIADO'. Cuando llega el ACK, quedan en 'CONFIRMADO'.
--
--    Ciclo:  PENDIENTE -> ENVIADO -> CONFIRMADO   (o FALLIDO, con su motivo)
-- ----------------------------------------------------------------------------
CREATE TABLE mensajes (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  cmd_id      VARCHAR(36) NOT NULL,                  -- UUID: empareja el ACK
  node_id     VARCHAR(32) NOT NULL,
  accion      ENUM('MENSAJE','SET_INTERVAL',
                   'SET_RECURSOS','PING','SOLICITAR_SYNC')
                 NOT NULL DEFAULT 'MENSAJE',
  texto       VARCHAR(255) NULL,
  valor       INT NULL,                              -- para SET_INTERVAL
  estado      ENUM('PENDIENTE','ENVIADO','CONFIRMADO','FALLIDO')
                 NOT NULL DEFAULT 'PENDIENTE',
  detalle     VARCHAR(255) NULL,                     -- por que fallo
  creado_en   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  enviado_en  DATETIME(3) NULL,
  ack_en      DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_cmd_id (cmd_id),
  KEY ix_pendientes (estado, creado_en),             -- lo lee el despachador
  KEY ix_nodo (node_id, creado_en DESC),
  CONSTRAINT fk_mensajes_nodo FOREIGN KEY (node_id)
      REFERENCES nodos (node_id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- La diferencia entre ack_en y enviado_en es el round-trip real del mensaje.
-- Es un dato lindo para mostrar en el dashboard y para la defensa.


-- ============================================================================
--  VISTAS
--  Sacan la logica repetida de Python y hacen la API mucho mas corta.
-- ============================================================================

-- Ultima metrica de CADA nodo.
--
-- Version anterior: ROW_NUMBER() OVER (PARTITION BY node_id ...) sobre toda la
-- tabla. Correcto, pero MySQL tiene que materializar la historia entera antes
-- de quedarse con nueve filas: con 150.000 metricas medimos ~0,9 s por consulta
-- y el indice ix_nodo_tiempo no se usaba nunca.
--
-- Version actual: se arranca desde `nodos` (9 filas) y para cada una se busca
-- su ultima metrica con una subconsulta que SI usa ix_nodo_tiempo. Nueve
-- busquedas por indice en vez de un recorrido completo.
--
-- Se ordena por (timestamp DESC, id DESC): sin el desempate por id, dos
-- metricas del mismo nodo en el mismo milisegundo darian un resultado
-- arbitrario y distinto en cada consulta.
--
-- El FORCE INDEX no es capricho. Sin el, el optimizador prefiere ix_tiempo y
-- recorre el indice de fechas hacia atras hasta topar con el nodo buscado. Eso
-- va bien mientras todos los nodos reporten... y se degrada justo con el nodo
-- que dejo de reportar, que es el caso que esta practica tiene que manejar.
-- Medido con 180.000 filas y un nodo caido: 154 ms sin hint, 62 ms con hint, y
-- la diferencia crece con el tiempo que lleve caido.
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


-- Ultima medicion de CADA recurso de cada nodo (v2).
-- Mismo truco que arriba: se arranca desde la lista de recursos distintos y se
-- busca el id mas reciente de cada uno por indice, en vez de recorrer todo.
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


-- Estado completo de cada nodo: catalogo + su ultima medicion.
-- Es lo que consume GET /api/nodes casi tal cual.
CREATE OR REPLACE VIEW v_nodos_estado AS
SELECT
    n.node_id,
    n.region,
    n.sede,
    n.hostname,
    n.sistema_operativo,
    n.ip,
    n.estado,
    n.intervalo_seg,
    n.primer_registro,
    n.ultimo_reporte,
    n.agente_version,
    n.capacidades,
    n.recursos_pedidos,
    n.ultima_desconexion,
    n.motivo_desconexion,
    n.ultima_reconexion,
    n.intermitente,
    n.caidas_recientes,
    n.ultima_seq,
    n.pendientes_sync,
    n.desvio_reloj_seg,
    um.disco_nombre,
    um.disco_tipo,
    um.total_gb,
    um.usado_gb,
    um.libre_gb,
    um.uso_pct,
    um.iops_lectura,
    um.iops_escritura,
    um.latencia_ms,
    um.origen                                        AS origen_ultima_metrica,
    TIMESTAMPDIFF(SECOND, n.ultimo_reporte, NOW()) AS segundos_sin_reportar,
    (SELECT COUNT(*) FROM eventos e
      WHERE e.node_id = n.node_id AND e.tipo = 'NO_REPORTA') AS failover_events,
    -- Cuantos recursos extra (RAM, CPU, otros discos) esta reportando hoy.
    (SELECT COUNT(*) FROM v_recursos_ultimo vr
      WHERE vr.node_id = n.node_id)                          AS recursos_activos,
    -- Capacidad TOTAL de almacenamiento del nodo contando los discos
    -- adicionales. El pendrive de Santa Cruz suma aqui, no en total_gb, que
    -- por definicion del enunciado es solo el primer disco.
    COALESCE((SELECT SUM(vr.total_gb) FROM v_recursos_ultimo vr
               WHERE vr.node_id = n.node_id AND vr.tipo = 'DISCO'), 0)
                                                             AS extra_disco_gb
FROM nodos n
LEFT JOIN v_ultima_metrica um ON um.node_id = n.node_id;


-- Consolidado del cluster: una sola fila con todos los totales.
-- Solo suma nodos ACTIVOS: un nodo caido no aporta capacidad disponible.
CREATE OR REPLACE VIEW v_cluster AS
SELECT
    (SELECT COUNT(*) FROM nodos)                              AS nodos_totales,
    (SELECT COUNT(*) FROM nodos WHERE estado = 'ACTIVO')      AS nodos_activos,
    (SELECT COUNT(*) FROM nodos WHERE intermitente = 1)       AS nodos_intermitentes,
    (SELECT COUNT(DISTINCT region) FROM nodos)                AS regionales,
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
    , 3)                                                      AS latencia_ponderada_ms,
    -- v2: capacidad contando discos adicionales (pendrives, discos externos).
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN total_gb + extra_disco_gb END), 0)
                                                              AS capacidad_con_extras_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN extra_disco_gb END), 0)
                                                              AS extras_gb
FROM v_nodos_estado;

-- NULLIF evita la division por cero cuando todavia no reporto nadie.
-- Si les preguntan "que pasa si arrancan con cero nodos", la respuesta esta aca:
-- devuelve una fila con ceros y NULL, no un error.


-- Consolidado POR REGIONAL (v2).
-- Existe porque La Paz tiene dos servidores: el enunciado habla de nueve
-- administraciones regionales, no de nueve maquinas. Esta vista responde
-- "cuanto almacenamiento tiene la regional La Paz" sumando sus dos nodos.
CREATE OR REPLACE VIEW v_regionales AS
SELECT
    region,
    COUNT(*)                                                  AS nodos,
    GROUP_CONCAT(sede ORDER BY sede SEPARATOR ' · ')          AS sedes,
    SUM(estado = 'ACTIVO')                                    AS nodos_activos,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) AS capacidad_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) AS usado_total_gb,
    COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN libre_gb END), 0) AS libre_total_gb,
    ROUND(
      COALESCE(SUM(CASE WHEN estado='ACTIVO' THEN usado_gb END), 0) /
      NULLIF(SUM(CASE WHEN estado='ACTIVO' THEN total_gb END), 0) * 100
    , 2)                                                      AS uso_pct,
    MAX(ultimo_reporte)                                       AS ultimo_reporte
FROM v_nodos_estado
GROUP BY region;


-- ============================================================================
--  PARTE 3 de 3  ·  COMPROBACION
-- ============================================================================

-- Si esto devuelve 5 tablas y 5 vistas, la base quedo bien.
SELECT TABLE_TYPE AS tipo, COUNT(*) AS cantidad
  FROM information_schema.TABLES
 WHERE TABLE_SCHEMA = 'cns_cluster'
 GROUP BY TABLE_TYPE;

-- Y esto tiene que devolver una fila con ceros: la base esta creada y vacia,
-- esperando a que se conecte el primer nodo. Que devuelva una fila y no un
-- error es la prueba de que v_cluster aguanta el cluster vacio — es una
-- pregunta tipica de la defensa.
SELECT * FROM v_cluster;

SELECT 'Base cns_cluster lista. Ahora: copiar la clave al .env y correr '
       'python -m db.probar_bd' AS siguiente_paso;
