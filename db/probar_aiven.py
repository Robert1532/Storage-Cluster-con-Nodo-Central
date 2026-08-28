"""
Verificador de conexion a Aiven — corrarlo ANTES que cualquier otra cosa.
Responsable: Robert (Datos).

    python -m db.probar_aiven

Comprueba, en orden, las cuatro cosas que pueden fallar:
  1. DNS: que el host de Aiven resuelva
  2. TCP: que el puerto este abierto desde esta red (los laboratorios y
     algunas redes de universidad bloquean puertos altos de salida)
  3. TLS + credenciales: que el ca.pem y la contrasena sean correctos
  4. Esquema: que las 5 tablas y las 5 vistas ya existan (v2)

Si el paso 2 falla en el aula el dia de la defensa, no hay demo contra Aiven:
por eso desde el dia 6 se trabaja contra MySQL local. Ver el documento del
equipo, seccion 6.
"""
from __future__ import annotations

import socket
import sys
import time

from comun import config


def paso(n: int, texto: str) -> None:
    print(f"\n[{n}/4] {texto}")


def main() -> int:
    print("=" * 66)
    print(" VERIFICACION DE CONEXION A LA BASE DE DATOS")
    print("=" * 66)
    print(f" host  : {config.DB_HOST}")
    print(f" puerto: {config.DB_PORT}")
    print(f" base  : {config.DB_NAME}")
    print(f" TLS   : {config.DB_SSL_CA or 'sin TLS (MySQL local)'}")

    # ---------------------------------------------------------------- 1 DNS
    paso(1, "Resolviendo el nombre del host...")
    try:
        ip = socket.gethostbyname(config.DB_HOST)
        print(f"      OK -> {ip}")
    except socket.gaierror as e:
        print(f"      FALLA: no resuelve. {e}")
        print("      Revisa que tengas internet y que el host este bien escrito.")
        return 1

    # ---------------------------------------------------------------- 2 TCP
    paso(2, f"Abriendo el puerto {config.DB_PORT}...")
    t0 = time.time()
    try:
        with socket.create_connection((config.DB_HOST, config.DB_PORT), timeout=15):
            pass
        print(f"      OK -> puerto abierto ({(time.time()-t0)*1000:.0f} ms de ida y vuelta)")
    except (socket.timeout, OSError) as e:
        print(f"      FALLA: {e}")
        print("      La red de donde estas conectando bloquea el puerto de salida.")
        print("      Probalo desde otra red (datos del celular sirve para descartar).")
        return 1

    # ------------------------------------------------------- 3 TLS + login
    paso(3, "Autenticando (TLS + usuario)...")
    try:
        from db.conexion import cursor
        with cursor() as cur:
            cur.execute("SELECT VERSION() AS v, @@max_connections AS mc, DATABASE() AS db")
            f = cur.fetchone()
            print(f"      OK -> MySQL {f['v']}  ·  base '{f['db']}'  ·  "
                  f"max_connections={f['mc']}")
            mayor, menor, parche = (f["v"].split("-")[0].split(".") + ["0", "0"])[:3]
            if (int(mayor), int(menor), int(parche)) < (8, 0, 14):
                print("      AVISO: las vistas necesitan MySQL 8.0.14 o superior.")
    except Exception as e:                                        # noqa: BLE001
        print(f"      FALLA: {type(e).__name__}: {str(e)[:200]}")
        print("      Revisa DB_USER, DB_PASSWORD y que DB_SSL_CA apunte al ca.pem.")
        return 1

    # ------------------------------------------------------------ 4 esquema
    paso(4, "Buscando las tablas y las vistas...")
    from db.conexion import cursor
    with cursor() as cur:
        cur.execute("SHOW TABLES")
        hay = {list(f.values())[0] for f in cur.fetchall()}

    faltan = [n for n in ("nodos", "metricas", "eventos", "mensajes", "recursos",
                          "v_ultima_metrica", "v_nodos_estado", "v_cluster",
                          "v_recursos_ultimo", "v_regionales")
              if n not in hay]
    if faltan:
        print(f"      Faltan: {', '.join(faltan)}")
        print("      Si la base esta vacia, corre el esquema completo;")
        print("      si ya tenia datos de la v1, corre db/migracion_v2.sql.")
        print(f"        mysql --host={config.DB_HOST} --port={config.DB_PORT} \\")
        print(f"              --user={config.DB_USER} --password \\")
        print(f"              --ssl-ca=db/ca.pem {config.DB_NAME} < db/schema.sql")
        return 1

    print("      OK -> las 5 tablas y las 5 vistas estan creadas")
    print("\n" + "=" * 66)
    print(" TODO EN ORDEN. Ahora podes correr:  python -m db.probar_bd")
    print("=" * 66)
    return 0


if __name__ == "__main__":
    try:
        codigo = main()
    finally:
        # Este archivo predica que un hilo que termina cierra su conexion.
        # Predicar con el ejemplo.
        try:
            from db.conexion import cerrar_conexion_del_hilo
            cerrar_conexion_del_hilo()
        except Exception:
            pass
    sys.exit(codigo)
