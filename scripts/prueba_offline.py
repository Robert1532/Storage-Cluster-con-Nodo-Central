"""
Prueba SIN base de datos — v2.  Responsable: Robert.

    python scripts/prueba_offline.py

Por que existe: las otras dos pruebas (db.probar_bd y prueba_integracion)
necesitan MySQL levantado. Esta no necesita nada mas que Python y psutil, asi
que cualquiera del equipo puede correrla en su maquina antes de subir un
cambio, y sirve como red de seguridad de las tres piezas mas delicadas de la
version 2:

  1. El FRAMING del protocolo, que es la pregunta 3 de la defensa.
  2. El FECHADO por reloj monotonico, que es lo que hace que cambiar la hora
     del cliente no mueva ni una fila del historico.
  3. La BASE LOCAL del cliente, que es lo que hace que una caida de red deje
     un hueco temporal y no permanente.

No reemplaza a las otras dos: no toca MySQL, no levanta sockets reales entre
maquinas y no prueba el dashboard.
"""
from __future__ import annotations

import json
import re
import socket
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))

from cliente import metricas                                      # noqa: E402
from cliente.almacen import AlmacenLocal                          # noqa: E402
from comun import protocolo                                       # noqa: E402

fallos: list[str] = []


def check(nombre: str, condicion: bool, detalle: str = "") -> None:
    print(f"  [{'OK  ' if condicion else 'FALLA'}] {nombre}"
          + (f"  -> {detalle}" if detalle else ""))
    if not condicion:
        fallos.append(nombre)


# ============================================================ 1. FRAMING

def prueba_framing() -> None:
    """
    TCP no respeta los limites de los mensajes. Estas son las tres formas en
    que eso se rompe en la practica, y las tres tienen que sobrevivirse.
    """
    print("\n1. Framing del protocolo (pregunta 3 de la defensa)")
    a, b = socket.socketpair()
    try:
        lector = protocolo.LectorLineas(b, tam_buffer=8)   # buffer chico a
                                                           # proposito: fuerza
                                                           # los cortes
        # Tres mensajes PEGADOS en un solo write, mas uno partido por la mitad.
        m1 = json.dumps(protocolo.metric("N1", {"total_gb": 1}, seq=1)).encode()
        m2 = json.dumps(protocolo.ack("c-1", "N1")).encode()
        m3 = json.dumps(protocolo.pong("N1")).encode()
        a.sendall(m1 + b"\n" + m2 + b"\n" + m3[:10])
        a.sendall(m3[10:] + b"\n")
        # Una linea corrupta en el medio no puede tumbar la conexion.
        a.sendall(b"{ esto no es json }\n")
        a.sendall(b"\n")                                   # linea vacia
        a.sendall(b"[1,2,3]\n")                            # JSON que no es objeto
        a.sendall(json.dumps(protocolo.metric_ok(7)).encode() + b"\n")
        a.close()

        recibidos = list(lector)
        tipos = [m.get("tipo") for m in recibidos]
        check("Entrega los 4 mensajes validos, en orden",
              tipos == ["METRIC", "ACK", "PONG", "METRIC_OK"], str(tipos))
        check("Descarta la basura sin cortar la conexion",
              lector.descartadas == 2, str(lector.descartadas))
        check("Un mensaje partido por la mitad se rearma entero",
              recibidos[2].get("node_id") == "N1")
    finally:
        a.close(); b.close()


# ================================================ 2. FECHADO POR EL SERVIDOR

def prueba_fechado() -> None:
    """
    El requisito "no permitir el cambio de hora del cliente".

    Se simula un cliente cuyo reloj de pared esta un ano adelantado y que
    entrega un lote de muestras tomadas hace 10, 8 y 6 minutos.
    """
    print("\n2. La hora la pone el servidor (reloj del cliente irrelevante)")
    ahora = datetime(2026, 8, 27, 18, 0, 0, tzinfo=timezone.utc)
    mono_envio = 1_000_000_000_000          # instante del envio, en ns

    fechada = protocolo.fechar_muestra(
        mono_envio, mono_envio - 600 * 1_000_000_000, ahora)
    check("Una muestra de hace 10 min se fecha 10 min antes",
          fechada == ahora - timedelta(minutes=10), str(fechada))

    viva = protocolo.fechar_muestra(mono_envio, mono_envio - 2_000_000, ahora)
    check("Una metrica en vivo se fecha practicamente ahora",
          abs((viva - ahora).total_seconds()) < 0.01, str(viva))

    # Lo importante: el resultado NO depende de la hora del cliente. Se calcula
    # dos veces con timestamps de cliente absurdos y tiene que dar lo mismo.
    check("El resultado no depende del reloj de pared del cliente",
          protocolo.fechar_muestra(mono_envio, mono_envio - 600_000_000_000,
                                   ahora) == fechada)

    check("Una edad negativa (reloj manipulado) cae a 'ahora'",
          protocolo.fechar_muestra(mono_envio, mono_envio + 5_000_000_000,
                                   ahora) == ahora)
    diez_anos_ns = 10 * 365 * 24 * 3600 * 1_000_000_000
    check("Una edad absurda (10 anos) cae a 'ahora'",
          protocolo.fechar_muestra(mono_envio, mono_envio - diez_anos_ns,
                                   ahora) == ahora)
    check("El tope de antiguedad es de un mes",
          protocolo.MAX_EDAD_MUESTRA_SEG == 30 * 24 * 3600)
    check("Sin mono_ns (cliente v1) cae a 'ahora'",
          protocolo.fechar_muestra(None, None, ahora) == ahora)

    # El orden relativo se conserva: es lo que hace que el grafico dibuje la
    # curva bien y no un serrucho.
    lote = [protocolo.fechar_muestra(mono_envio,
                                     mono_envio - s * 1_000_000_000, ahora)
            for s in (600, 400, 200, 0)]
    check("Un lote queda REPARTIDO en el tiempo y en orden",
          lote == sorted(lote) and (lote[-1] - lote[0]).total_seconds() == 600)

    check("mono_ns() avanza siempre",
          protocolo.mono_ns() <= protocolo.mono_ns())


# =========================================================== 3. RECURSOS

def prueba_recursos() -> None:
    """Lo que llega por la red no se cree: se sanea antes de tocar la base."""
    print("\n3. Recursos flexibles y saneado de lo que llega por la red")
    r = protocolo.recurso("RAM", "fisica",
                          {"total_gb": 16.0, "usado_gb": 9.5, "basura": "hola",
                           "nan": float("nan"), "inf": float("inf"),
                           "booleano": True},
                          {"unidad": "GB", "nada": None})
    check("Descarta los valores que no son numeros",
          set(r["metricas"]) == {"total_gb", "usado_gb"}, str(list(r["metricas"])))
    check("Un tipo inventado cae a CUSTOM",
          protocolo.recurso("PLASMA", "x", {})["tipo"] == "CUSTOM")

    sucios = [
        {"tipo": "DISCO", "nombre": "C:\\", "metricas": {"total_gb": 500}},
        {"tipo": "RAM"},                              # sin nombre: se descarta
        "esto no es un dict",                          # se descarta
        {"nombre": "x", "metricas": "tampoco es dict"},
    ]
    limpios = protocolo.validar_recursos(sucios)
    check("Descarta los recursos mal formados",
          len(limpios) == 2, str(len(limpios)))
    check("Nunca lanza con basura total",
          protocolo.validar_recursos("no soy una lista") == [])
    check("Corta una lista absurdamente larga",
          len(protocolo.validar_recursos(
              [{"tipo": "CPU", "nombre": f"n{i}", "metricas": {}}
               for i in range(500)])) == protocolo.MAX_RECURSOS_MUESTRA)

    largo = protocolo.recurso("CPU", "x" * 300, {"a" * 300: 1})
    check("Recorta nombres mas largos que la columna",
          len(largo["nombre"]) <= protocolo.MAX_NOMBRE_RECURSO)


# ====================================================== 4. BASE LOCAL

def prueba_almacen() -> None:
    """
    Lo que hace que una caida de red deje un hueco TEMPORAL y no permanente.
    """
    print("\n4. Base local del cliente (store and forward)")
    with tempfile.TemporaryDirectory() as tmp:
        ruta = Path(tmp) / "prueba.db"
        al = AlmacenLocal("TEST-OFF", ruta=ruta, max_muestras=10,
                          retencion_horas=1)

        disco = {"nombre": "/", "tipo": "SSD", "total_gb": 500.0,
                 "usado_gb": 200.0, "libre_gb": 300.0, "uso_pct": 40.0,
                 "iops_lectura": 5, "iops_escritura": 3, "latencia_ms": 0.4}
        recursos = [protocolo.recurso("RAM", "fisica", {"total_gb": 16.0})]

        seqs = [al.guardar(disco, recursos) for _ in range(5)]
        check("Los seq crecen siempre", seqs == sorted(seqs) and len(set(seqs)) == 5,
              str(seqs))
        check("Todo queda pendiente mientras no haya confirmacion",
              al.contar_pendientes() == 5, str(al.contar_pendientes()))

        lote = al.pendientes(3)
        check("pendientes() devuelve de mas viejo a mas nuevo",
              [m["seq"] for m in lote] == seqs[:3], str([m["seq"] for m in lote]))
        check("Cada muestra trae su mono_ns y sus recursos",
              all(m["mono_ns"] > 0 and m["recursos"] for m in lote))

        # El SYNC_OK es ACUMULATIVO: confirma todo lo <= a ese seq.
        al.marcar_entregadas(seqs[2])
        check("marcar_entregadas confirma de forma acumulativa",
              al.contar_pendientes() == 2, str(al.contar_pendientes()))
        check("Lo confirmado ya no vuelve a salir como pendiente",
              [m["seq"] for m in al.pendientes(10)] == seqs[3:])

        # La muestra confirmada NO se borra: sigue siendo el historial local
        # del nodo ("va guardando el comportamiento del disco").
        check("Lo confirmado sigue guardado como historial local",
              al.resumen()["muestras"] == 5, str(al.resumen()["muestras"]))

        # Poda: se pasa del maximo y tiene que recortar sin romperse.
        for _ in range(20):
            al.guardar(disco, [])
        al.podar()
        r = al.resumen()
        check("La poda respeta el techo de muestras",
              r["muestras"] <= 10, str(r["muestras"]))

        # AUTOINCREMENT: despues de podar, un seq NUNCA se reutiliza. Si se
        # reutilizara, el servidor descartaria datos nuevos creyendo que son
        # duplicados.
        nuevo = al.guardar(disco, [])
        check("Un seq nunca se reutiliza despues de podar",
              nuevo > max(seqs) + 20, str(nuevo))

        al.anotar("MENSAJE", "Reinicie servicio")
        al.anotar("SYNC", "12 muestras recuperadas")
        anotaciones = al.ultimas_anotaciones(10)
        tipos = {a["tipo"] for a in anotaciones}
        check("La bitacora guarda lo recibido (requisito 7.1, consultable)",
              {"MENSAJE", "SYNC"} <= tipos, str(sorted(tipos)))
        check("La bitacora vuelve la mas reciente primero",
              anotaciones[0]["tipo"] == "SYNC", anotaciones[0]["tipo"])
        # La poda de arriba tiro muestras SIN ENVIAR. Que quede constancia no
        # es un detalle: es la diferencia entre un hueco explicado y un hueco
        # que alguien descubre por casualidad tres semanas despues.
        check("Una perdida por buffer lleno queda anotada",
              "BUFFER_LLENO" in tipos, str(sorted(tipos)))

        al.guardar_estado("ultimo_ack", "42")
        check("El estado sobrevive entre reinicios del cliente",
              al.leer_estado("ultimo_ack") == "42")
        al.cerrar()

        # Reabrir: es lo que pasa cuando el cliente se reinicia.
        al2 = AlmacenLocal("TEST-OFF", ruta=ruta)
        check("Al reabrir, lo pendiente sigue ahi",
              al2.contar_pendientes() > 0, str(al2.contar_pendientes()))
        check("Y el seq sigue creciendo desde donde iba",
              al2.guardar(disco, []) > nuevo)
        al2.cerrar()


# ======================================================= 5. COLECTORES

def prueba_colectores() -> None:
    """Ningun sensor puede lanzar: uno que revienta tumba el nodo entero."""
    print("\n5. Colectores de metricas (disco, RAM, CPU, red)")
    metricas.sembrar()
    time.sleep(1.1)                       # los deltas necesitan dos lecturas

    d = metricas.leer_disco()
    claves = {"nombre", "tipo", "total_gb", "usado_gb", "libre_gb", "uso_pct",
              "iops_lectura", "iops_escritura", "latencia_ms"}
    check("leer_disco devuelve exactamente las claves del protocolo",
          set(d) == claves, str(set(d) ^ claves))
    check("El tipo de disco es uno de los del ENUM",
          d["tipo"] in protocolo.TIPOS_DISCO, str(d["tipo"]))
    check("La capacidad es un numero positivo",
          isinstance(d["total_gb"], (int, float)) and d["total_gb"] > 0,
          str(d["total_gb"]))
    check("El % de uso esta entre 0 y 100",
          0 <= d["uso_pct"] <= 100, str(d["uso_pct"]))

    caps = metricas.capacidades()
    check("El nodo anuncia lo que sabe medir",
          {"disco", "ram", "cpu", "red", "discos"} <= set(caps), str(caps))

    todos = metricas.leer_recursos(["discos", "ram", "cpu", "red"])
    tipos = {r["tipo"] for r in todos}
    check("Mide RAM y CPU ademas del disco",
          {"RAM", "CPU"} <= tipos, str(tipos))
    check("Todo recurso trae nombre y metricas numericas",
          all(r["nombre"] and all(isinstance(v, (int, float))
                                  for v in r["metricas"].values())
              for r in todos))

    ram = next((r for r in todos if r["tipo"] == "RAM"), None)
    check("La RAM usa las mismas claves que un disco (total/usado/uso_pct)",
          ram is not None and {"total_gb", "usado_gb", "uso_pct"} <= set(ram["metricas"]))

    disc = [r for r in todos if r["tipo"] == "DISCO"]
    check("Reporta TODAS las unidades, no solo la primera",
          len(disc) >= 1, f"{len(disc)} unidades")
    check("Cada unidad dice si es extraible (pendrive) y si es la principal",
          all("removible" in r["etiquetas"] and "principal" in r["etiquetas"]
              for r in disc))

    check("Un recurso desconocido se ignora sin romper nada",
          metricas.leer_recursos(["no_existe"]) == [])
    check("detectar_cambios_discos no lanza y no inventa cambios",
          metricas.detectar_cambios_discos() == [])

    # El mensaje completo tal como viaja por el socket.
    m = protocolo.metric("CNS-LPZ-01", d, seq=1, recursos=todos)
    texto = json.dumps(m, ensure_ascii=False)
    check("El METRIC completo serializa a JSON en una linea",
          "\n" not in texto and len(texto) < protocolo.MAX_LINEA, f"{len(texto)} bytes")

    lote = protocolo.metric_batch("CNS-LPZ-01",
                                  [protocolo.muestra(i, d, todos) for i in range(200)])
    check("Un lote se corta en el maximo configurado",
          len(lote["muestras"]) == protocolo.MAX_MUESTRAS_LOTE,
          str(len(lote["muestras"])))
    check("Un lote lleno sigue entrando en una linea",
          len(json.dumps(lote)) < protocolo.MAX_LINEA,
          f"{len(json.dumps(lote))} bytes")


# ================================================== 6. DASHBOARD (sintaxis)

def prueba_dashboard() -> None:
    """
    Que el JavaScript del dashboard al menos PARSEE.

    Por que existe: un error de sintaxis en ese archivo no rompe nada del lado
    de Python — las pruebas siguen en verde, el servidor arranca, la API
    responde — y la pantalla queda en blanco. Se descubre recien cuando alguien
    la abre, que en el peor caso es durante la defensa.

    Ya paso: al agregar un plural en una funcion se declaro `const d` cuando `d`
    ya existia unas lineas arriba. El navegador tira "Identifier 'd' has already
    been declared" y NO EJECUTA NADA del archivo.

    Se usa Node si esta instalado. Si no esta, se avisa y se hacen las
    comprobaciones que si se pueden hacer sin el.
    """
    import shutil
    import subprocess

    print("\n6. Dashboard: el JavaScript parsea y los ids existen")
    ruta = RAIZ / "dashboard" / "index.html"
    html = ruta.read_text(encoding="utf-8")

    m = re.search(r'<script>\n"use strict";([\s\S]*?)</script>', html)
    check("Se encuentra el bloque de JavaScript", m is not None)
    if not m:
        return
    codigo = m.group(1)

    if shutil.which("node"):
        r = subprocess.run(["node", "-e",
                            "new Function(require('fs').readFileSync(0,'utf8'))"],
                           input=codigo, capture_output=True, text=True, timeout=30)
        check("El JavaScript parsea sin errores de sintaxis", r.returncode == 0,
              (r.stderr or "").strip().splitlines()[-1] if r.returncode else "")
    else:
        print("      (Node no esta instalado: no se puede parsear el JS)")

    # Todo getElementById tiene que apuntar a un id que exista en el HTML, o a
    # uno que el propio JS cree. Un id mal escrito no lanza: devuelve null y la
    # pantalla se queda a medio dibujar sin decir por que.
    ids_html = set(re.findall(r'id="([^"]+)"', html))
    usados = set(re.findall(r'\$\("([^"]+)"\)', codigo))
    usados |= set(re.findall(r'getElementById\("([^"]+)"\)', codigo))
    faltan = sorted(usados - ids_html)
    check("Todos los ids que busca el JS existen en el HTML",
          not faltan, ", ".join(faltan))

    # Las rutas de la API que usa el dashboard tienen que estar declaradas.
    rutas_js = set(re.findall(r'["`](/api/[a-z\-]+)', codigo))
    api = (RAIZ / "api" / "main.py").read_text(encoding="utf-8")
    declaradas = set(re.findall(r'@app\.(?:get|post)\("([^"]+)"', api))
    sueltas = [r for r in rutas_js
               if r not in declaradas and not any(d.startswith(r) for d in declaradas)]
    check("Las rutas que llama el dashboard existen en la API",
          not sueltas, ", ".join(sueltas))

    check("El dashboard sigue teniendo respaldo REST si el WebSocket falla",
          "cargarREST" in codigo and "onclose" in codigo)


def main() -> int:
    print("=" * 70)
    print(" PRUEBA OFFLINE (sin MySQL) - Storage Cluster CNS v2")
    print("=" * 70)
    prueba_framing()
    prueba_fechado()
    prueba_recursos()
    prueba_almacen()
    prueba_colectores()
    prueba_dashboard()

    print("\n" + "=" * 70)
    if fallos:
        print(f" RESULTADO: {len(fallos)} fallas")
        for f in fallos:
            print(f"   - {f}")
        return 1
    print(" RESULTADO: todo OK.")
    print(" Falta correr, con MySQL levantado:")
    print("   python -m db.probar_bd")
    print("   python scripts/prueba_integracion.py")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
