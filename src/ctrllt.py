#!/usr/bin/env python3
"""
ctrllt.py — Controlador de Lotes (pasarela central del sistema)

Qué hace este servicio:
  Es el corazón del sistema. Recibe peticiones de los clientes a través de
  tuberías nombradas, inspecciona el campo "servicio" del mensaje JSON y lo
  reenvía al servicio interno correspondiente (gesfich, gesprog o ejecutor).
  Luego espera la respuesta de ese servicio y la reenvía al cliente.

  Su función es de PASARELA (gateway): no procesa el contenido de las
  peticiones, solo las enruta. La única operación propia es "Terminar",
  que apaga todo el sistema.

Cómo encaja en el sistema:
  cliente ──[pipe_cli_req]──▶ ctrllt ──[pipe_fich_req]──▶ gesfich
  cliente ◀──[pipe_cli_res]── ctrllt ◀──[pipe_fich_res]── gesfich
                              ctrllt ──[pipe_prog_req]──▶ gesprog
                              ctrllt ◀──[pipe_prog_res]── gesprog
                              ctrllt ──[pipe_ejec_req]──▶ ejecutor
                              ctrllt ◀──[pipe_ejec_res]── ejecutor

  El cliente SOLO habla con ctrllt. Los servicios internos NUNCA hablan
  entre sí directamente.

Manejo de múltiples clientes:
  En Linux, varios procesos pueden escribir en la misma FIFO.
  Las escrituras de hasta PIPE_BUF bytes (4096 en Linux, igual a MSG_MAX_LEN)
  son ATÓMICAS: el kernel garantiza que ninguna escritura se entrelaza con
  otra. Así, si dos clientes envían peticiones simultáneas, cada mensaje
  llega completo e íntegro a ctrllt. Las respuestas, sin embargo, se envían
  en el orden en que llegan las peticiones (procesamiento secuencial).
  Esto significa que si dos clientes envían a la vez, sus respuestas pueden
  mezclarse en el pipe de respuestas. Para evitar esto en un entorno de
  producción, cada cliente usaría su propio par de pipes (con sufijo de PID).
  Para este proyecto académico, el procesamiento secuencial es suficiente.

Nota sobre el argumento -c de gesprog:
  El enunciado especifica -c para la pipe de respuestas de gesprog, pero -c
  ya se usa para la pipe de peticiones del cliente. Para evitar este conflicto
  en argparse, se usa --gres para la respuesta de gesprog. En la práctica,
  el par de pipes se nombra explícitamente al lanzar el sistema (ver README).

Sinopsis:
  python3 ctrllt.py -c <pipe_cli_req> [-a <pipe_cli_res>]
                    -f <pipe_fich_req> [-b <pipe_fich_res>]
                    -p <pipe_prog_req> [--gres <pipe_prog_res>]
                    -e <pipe_ejec_req> [-d <pipe_ejec_res>]

Máquina de estados:
  inicio → Corriendo ──Terminar──▶ Terminado
"""

import os
import sys
import json
import argparse
import signal

# ── Constantes del protocolo ──────────────────────────────────────────────────
MSG_MAX_LEN = 4096  # Máximo tamaño de mensaje JSON en bytes

# ── Estados de ctrllt ─────────────────────────────────────────────────────────
ESTADO_CORRIENDO = "Corriendo"
ESTADO_TERMINADO = "Terminado"


# =============================================================================
# ARGUMENTOS DE LÍNEA DE COMANDOS
# =============================================================================

def parsear_argumentos():
    """
    Parsea los argumentos de ctrllt según la sinopsis del enunciado.

    Nota sobre el conflicto de -c:
      El enunciado usa -c tanto para el pipe del cliente como para el pipe
      de respuesta de gesprog. Esto parece un error tipográfico en el PDF.
      Aquí se usa --gres para el pipe de respuesta de gesprog.

    Retorna:
        argparse.Namespace con todos los pipes configurados
    """
    parser = argparse.ArgumentParser(
        description="ctrllt — Controlador de Lotes: pasarela central del sistema"
    )
    # Pipes para la conexión con el cliente
    parser.add_argument("-c", required=True,  metavar="<pipe>",
                        help="Pipe de PETICIONES del cliente hacia ctrllt")
    parser.add_argument("-a", required=False, metavar="<pipe>", default=None,
                        help="Pipe de RESPUESTAS de ctrllt hacia el cliente (half-duplex)")

    # Pipes para la conexión con gesfich
    parser.add_argument("-f", required=True,  metavar="<pipe>",
                        help="Pipe de PETICIONES de ctrllt hacia gesfich")
    parser.add_argument("-b", required=False, metavar="<pipe>", default=None,
                        help="Pipe de RESPUESTAS de gesfich hacia ctrllt (half-duplex)")

    # Pipes para la conexión con gesprog
    parser.add_argument("-p", required=True,  metavar="<pipe>",
                        help="Pipe de PETICIONES de ctrllt hacia gesprog")
    parser.add_argument("--gres", required=False, metavar="<pipe>", default=None,
                        help="Pipe de RESPUESTAS de gesprog hacia ctrllt (half-duplex)")

    # Pipes para la conexión con ejecutor
    parser.add_argument("-e", required=True,  metavar="<pipe>",
                        help="Pipe de PETICIONES de ctrllt hacia ejecutor")
    parser.add_argument("-d", required=False, metavar="<pipe>", default=None,
                        help="Pipe de RESPUESTAS de ejecutor hacia ctrllt (half-duplex)")

    return parser.parse_args()


# =============================================================================
# GESTIÓN DE TUBERÍAS NOMBRADAS
# =============================================================================

def crear_pipe_si_no_existe(ruta):
    """
    Crea una FIFO en el sistema de archivos si no existe ya.

    ctrllt es responsable de crear las pipes del cliente (según el enunciado:
    "Este servicio se encarga de crear la(s) tubería(s) que requiera conectarse
    con el cliente"). Las pipes de los servicios internos las crean los propios
    servicios (gesfich, gesprog, ejecutor).

    Parámetros:
        ruta: path donde crear la FIFO, o None para ignorar
    """
    if ruta and not os.path.exists(ruta):
        os.mkfifo(ruta)
        print(f"[ctrllt] FIFO creada: {ruta}", flush=True)


def abrir_pipe(ruta, modo="rdwr"):
    """
    Abre una FIFO existente sin bloquear el proceso (O_RDWR).

    Por qué O_RDWR (ver también gesfich.py para la explicación completa):
      Los servicios internos (gesfich, gesprog, ejecutor) también abren sus
      pipes con O_RDWR, así que no importa el orden de arranque. Si ctrllt
      abriera con O_WRONLY y el servicio todavía no hubiera abierto su extremo
      de lectura, ctrllt quedaría bloqueado. Con O_RDWR, no hay bloqueo.

    Parámetros:
        ruta: path de la FIFO a abrir, o None
        modo: ignorado (siempre O_RDWR); se mantiene por legibilidad

    Retorna:
        Descriptor de archivo, o None si ruta es None
    """
    if ruta is None:
        return None
    return os.open(ruta, os.O_RDWR)


def leer_mensaje(fd_lectura):
    """
    Lee un mensaje JSON de una FIFO, byte a byte hasta '\\n'.

    Idéntico al leer_mensaje de los demás servicios. Ver gesfich.py para
    la explicación detallada de por qué se lee byte a byte y no en bloques.

    Parámetros:
        fd_lectura: descriptor del FIFO

    Retorna:
        Diccionario Python o None si hubo error
    """
    buffer = b""
    while True:
        try:
            byte = os.read(fd_lectura, 1)
        except OSError as e:
            print(f"[ctrllt] Error leyendo pipe: {e}", flush=True)
            return None

        if not byte:
            return None

        if byte == b"\n":
            break

        buffer += byte

        if len(buffer) > MSG_MAX_LEN:
            print(f"[ctrllt] ADVERTENCIA: mensaje demasiado largo, descartando", flush=True)
            return None

    if not buffer:
        return None

    try:
        return json.loads(buffer.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"[ctrllt] JSON inválido: {e}", flush=True)
        return None


def enviar_mensaje(fd_escritura, datos):
    """
    Serializa y envía un diccionario como JSON + '\\n'.

    Parámetros:
        fd_escritura: descriptor del FIFO destino
        datos       : diccionario a serializar
    """
    linea = json.dumps(datos, ensure_ascii=False) + "\n"
    try:
        os.write(fd_escritura, linea.encode("utf-8"))
    except OSError as e:
        print(f"[ctrllt] Error enviando mensaje: {e}", flush=True)


# =============================================================================
# LÓGICA DE ENRUTAMIENTO
# =============================================================================

def obtener_fds_servicio(servicio, fds):
    """
    Retorna el par (fd_escritura, fd_lectura) para un servicio dado.

    fds es un diccionario con la estructura:
      {
        "gesfich":  (fd_req_fich, fd_res_fich),
        "gesprog":  (fd_req_prog, fd_res_prog),
        "ejecutor": (fd_req_ejec, fd_res_ejec)
      }
    donde fd_req es para enviar peticiones AL servicio,
          fd_res es para recibir respuestas DEL servicio.

    Parámetros:
        servicio: string con el nombre del servicio
        fds     : diccionario de descriptores de archivo por servicio

    Retorna:
        Tupla (fd_escritura, fd_lectura) o (None, None) si servicio inválido
    """
    return fds.get(servicio, (None, None))


def enrutar_peticion(peticion, fds_servicios):
    """
    Reenvía una petición al servicio indicado y retorna la respuesta.

    Flujo:
      1. Inspeccionar campo "servicio" del mensaje JSON
      2. Obtener los fds del servicio correspondiente
      3. Escribir el mensaje JSON al fd de peticiones del servicio
      4. Leer la respuesta del fd de respuestas del servicio
      5. Retornar la respuesta al llamador (que la enviará al cliente)

    Este es el corazón de la función de pasarela (gateway): ctrllt no
    modifica el contenido de las peticiones ni de las respuestas; solo
    las mueve de un pipe a otro.

    Por qué el enrutamiento es secuencial y no concurrente:
      Cada servicio interno tiene un solo par de pipes para comunicarse con
      ctrllt. Si ctrllt procesara varias peticiones en paralelo, podría
      enviar dos peticiones a gesfich antes de leer la primera respuesta, y
      las respuestas se mezclarían. El procesamiento secuencial garantiza que
      la respuesta que ctrllt lee es exactamente la que corresponde a la
      petición que acaba de enviar.

    Parámetros:
        peticion      : diccionario con la petición del cliente
        fds_servicios : {nombre_servicio → (fd_req, fd_res)}

    Retorna:
        Diccionario con la respuesta del servicio, o error si falla el enrutamiento
    """
    nombre_servicio = peticion.get("servicio", "")

    # Verificar que el servicio es uno de los conocidos
    if nombre_servicio not in fds_servicios:
        return {"estado": "error", "mensaje": "servicio desconocido"}

    fd_req_servicio, fd_res_servicio = fds_servicios[nombre_servicio]

    # Verificar que los fds de este servicio están conectados
    if fd_req_servicio is None or fd_res_servicio is None:
        return {"estado": "error", "mensaje": "servicio no conectado"}

    # Enviar la petición al servicio (sin modificar su contenido)
    try:
        enviar_mensaje(fd_req_servicio, peticion)
    except Exception as e:
        print(f"[ctrllt] Error enviando a {nombre_servicio}: {e}", flush=True)
        return {"estado": "error", "mensaje": "error enviando solicitud al servicio"}

    # Leer la respuesta del servicio
    # Esta llamada bloquea hasta que el servicio envíe su respuesta.
    # Como el procesamiento es secuencial, no hay riesgo de mezclar respuestas.
    respuesta = leer_mensaje(fd_res_servicio)

    if respuesta is None:
        return {"estado": "error", "mensaje": "error leyendo respuesta del servicio"}

    return respuesta


def op_terminar_sistema(fds_servicios):
    """
    Propaga el apagado a todos los servicios internos y termina ctrllt.

    Según el enunciado:
      1. Enviar "Terminar" a gesfich y gesprog
      2. Enviar "Parar" a ejecutor (se auto-detiene al quedar sin procesos)
      3. Retornar ok al cliente
      4. ctrllt sale del bucle principal

    Por qué Parar para ejecutor y Terminar para los otros:
      El ejecutor puede tener procesos corriendo. "Terminar" inmediato los
      dejaría huérfanos. "Parar" le indica que deje de aceptar nuevos procesos
      y que espere a que los actuales terminen antes de salir. Gesfich y gesprog
      no tienen subprocesos, así que "Terminar" es seguro.

    Parámetros:
        fds_servicios: {nombre_servicio → (fd_req, fd_res)}

    Retorna:
        {"estado": "ok"}
    """
    # Terminar gesfich
    if "gesfich" in fds_servicios:
        fd_req, fd_res = fds_servicios["gesfich"]
        if fd_req and fd_res:
            enviar_mensaje(fd_req, {"servicio": "gesfich", "operacion": "Terminar"})
            leer_mensaje(fd_res)  # Esperar confirmación antes de continuar
            print("[ctrllt] gesfich terminado", flush=True)

    # Terminar gesprog
    if "gesprog" in fds_servicios:
        fd_req, fd_res = fds_servicios["gesprog"]
        if fd_req and fd_res:
            enviar_mensaje(fd_req, {"servicio": "gesprog", "operacion": "Terminar"})
            leer_mensaje(fd_res)
            print("[ctrllt] gesprog terminado", flush=True)

    # Parar ejecutor (espera a que sus procesos activos terminen)
    if "ejecutor" in fds_servicios:
        fd_req, fd_res = fds_servicios["ejecutor"]
        if fd_req and fd_res:
            enviar_mensaje(fd_req, {"servicio": "ejecutor", "operacion": "Parar"})
            leer_mensaje(fd_res)
            print("[ctrllt] ejecutor en estado Parar", flush=True)

    return {"estado": "ok"}


# =============================================================================
# BUCLE PRINCIPAL
# =============================================================================

def main():
    """
    Punto de entrada de ctrllt.

    Flujo:
      1. Parsear argumentos
      2. Crear las pipes del cliente (responsabilidad de ctrllt)
      3. Abrir todos los pipes (cliente y servicios) con O_RDWR
      4. Entrar en el bucle: leer petición → enrutar → responder
      5. Si la petición es para ctrllt mismo (Terminar), apagar el sistema
    """
    args = parsear_argumentos()

    # ── Crear pipes del cliente (ctrllt es responsable de crearlas) ───────────
    # Las pipes de los servicios las crean los propios servicios (gesfich, etc.)
    crear_pipe_si_no_existe(args.c)
    crear_pipe_si_no_existe(args.a)

    # ── Abrir todos los pipes ─────────────────────────────────────────────────
    # Pipes del cliente: ctrllt LEERÁ peticiones y ESCRIBIRÁ respuestas
    fd_cli_req = abrir_pipe(args.c)
    fd_cli_res = abrir_pipe(args.a) if args.a else fd_cli_req

    # Pipes de gesfich: ctrllt ESCRIBE peticiones y LEE respuestas
    fd_fich_req = abrir_pipe(args.f)
    fd_fich_res = abrir_pipe(args.b) if args.b else fd_fich_req

    # Pipes de gesprog: ctrllt ESCRIBE peticiones y LEE respuestas
    fd_prog_req = abrir_pipe(args.p)
    fd_prog_res = abrir_pipe(args.gres) if args.gres else fd_prog_req

    # Pipes de ejecutor: ctrllt ESCRIBE peticiones y LEE respuestas
    fd_ejec_req = abrir_pipe(args.e)
    fd_ejec_res = abrir_pipe(args.d) if args.d else fd_ejec_req

    # Diccionario de servicios: mapea nombre → (fd_para_enviar, fd_para_recibir)
    # Se pasa a la función de enrutamiento para que sepa por qué pipes comunicarse
    fds_servicios = {
        "gesfich":  (fd_fich_req, fd_fich_res),
        "gesprog":  (fd_prog_req, fd_prog_res),
        "ejecutor": (fd_ejec_req, fd_ejec_res),
    }

    estado = ESTADO_CORRIENDO

    # Manejador de señal para cierre limpio
    todos_fds = [fd_cli_req, fd_cli_res, fd_fich_req, fd_fich_res,
                 fd_prog_req, fd_prog_res, fd_ejec_req, fd_ejec_res]

    def manejador_senal(signum, frame):
        print(f"\n[ctrllt] Señal {signum} recibida, cerrando...", flush=True)
        for fd in set(f for f in todos_fds if f is not None):
            try:
                os.close(fd)
            except OSError:
                pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, manejador_senal)
    signal.signal(signal.SIGINT,  manejador_senal)

    print(f"[ctrllt] Servicio iniciado. Escuchando en: {args.c}", flush=True)

    # ── Bucle principal de enrutamiento ──────────────────────────────────────
    while estado != ESTADO_TERMINADO:

        # Leer petición del cliente (bloqueante hasta que llegue algo)
        peticion = leer_mensaje(fd_cli_req)

        if peticion is None:
            continue

        nombre_servicio = peticion.get("servicio", "")
        operacion       = peticion.get("operacion", "")

        print(f"[ctrllt] Petición: servicio={nombre_servicio}, op={operacion}", flush=True)

        # ── Operación propia de ctrllt ────────────────────────────────────────
        if nombre_servicio == "ctrllt":
            if operacion == "Terminar":
                # Propagar el apagado a todos los servicios
                respuesta = op_terminar_sistema(fds_servicios)
                enviar_mensaje(fd_cli_res, respuesta)
                estado = ESTADO_TERMINADO
                # Seguir el bucle para que la condición de salida lo detecte
            else:
                respuesta = {"estado": "error", "mensaje": "operacion ctrllt desconocida"}
                enviar_mensaje(fd_cli_res, respuesta)
            continue

        # ── Enrutar la petición al servicio correspondiente ───────────────────
        # ctrllt actúa como proxy: reenvía el mensaje sin modificarlo y
        # reenvía la respuesta sin modificarla.
        respuesta = enrutar_peticion(peticion, fds_servicios)

        # Enviar la respuesta del servicio de vuelta al cliente
        enviar_mensaje(fd_cli_res, respuesta)

    # ── Limpieza final ────────────────────────────────────────────────────────
    print("[ctrllt] Servicio terminado.", flush=True)
    for fd in set(f for f in todos_fds if f is not None):
        try:
            os.close(fd)
        except OSError:
            pass


if __name__ == "__main__":
    main()
