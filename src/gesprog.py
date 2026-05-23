#!/usr/bin/env python3
"""
gesprog.py — Servicio Gestor de Programas

Qué hace este servicio:
  Registra, lee, actualiza y borra los metadatos de programas ejecutables
  almacenados en el área aralmac. Un "programa" es la imagen de un ejecutable
  con sus argumentos y variables de ambiente, identificado por un ID único
  (p-XXXX). El servicio NO ejecuta programas; eso lo hace el ejecutor.

Cómo encaja en el sistema:
  ctrllt recibe peticiones de los clientes y las reenvía a este servicio.
  El ejecutor también lee directamente del aralmac para obtener los metadatos
  del programa cuando va a lanzar un proceso de lotes.

  ctrllt ──[pipe_prog_req]──▶ gesprog
  ctrllt ◀──[pipe_prog_res]── gesprog

Almacenamiento (aralmac):
  aralmac/
  ├── programas/
  │   ├── p-0001.json   ← metadatos del programa
  │   ├── p-0002.json
  │   └── ...
  └── programas_counter.json  ← {"next": 3}

  Cada p-XXXX.json contiene:
  {
    "id-programa": "p-0001",
    "nombre":      "sort",          <- basename del ejecutable
    "ejecutable":  "/usr/bin/sort", <- ruta completa
    "args":        ["-r", "-n"],    <- argumentos (puede ser lista vacía)
    "env":         ["LANG=es"]      <- variables de entorno (puede ser lista vacía)
  }

Sinopsis:
  python3 gesprog.py -p <pipe_req> [-c <pipe_res>] -x <dir_aralmac>

Máquina de estados (diferencia clave con gesfich):
  En estado SUSPENDIDO, gesprog SIGUE aceptando la operación Leer.
  Esto está explícito en la figura 4 del enunciado: la flecha "Leer" apunta
  desde Suspendido hacia afuera, indicando que es la única operación CRUD
  permitida en ese estado.

  Corriendo ──Suspender──▶ Suspendido ──Reasumir──▶ Corriendo
  Corriendo  ──Terminar──▶ Terminado
  Suspendido ──Terminar──▶ Terminado
  Suspendido ──Leer──────▶ Suspendido  (permitida en suspensión)
"""

import os
import sys
import json
import argparse
import signal

# ── Constantes del protocolo ──────────────────────────────────────────────────
MSG_MAX_LEN = 4096  # Máximo tamaño de mensaje en bytes (definido en el enunciado)

# ── Estados de la máquina de estados ──────────────────────────────────────────
ESTADO_CORRIENDO  = "Corriendo"
ESTADO_SUSPENDIDO = "Suspendido"
ESTADO_TERMINADO  = "Terminado"


# =============================================================================
# ARGUMENTOS DE LÍNEA DE COMANDOS
# =============================================================================

def parsear_argumentos():
    """
    Parsea los argumentos de línea de comandos según la sinopsis del enunciado.

    Retorna:
        argparse.Namespace con:
          - p  : ruta de la FIFO de peticiones (gesprog lee de aquí)
          - c  : ruta de la FIFO de respuestas (gesprog escribe aquí; half-duplex)
          - x  : ruta del directorio aralmac
    """
    parser = argparse.ArgumentParser(
        description="gesprog — Servicio de Gestión de Programas del sistema de lotes"
    )
    parser.add_argument(
        "-p", required=True, metavar="<tuberia-req>",
        help="Tubería nombrada para recibir peticiones de ctrllt"
    )
    parser.add_argument(
        "-c", required=False, default=None, metavar="<tuberia-res>",
        help="Tubería nombrada para enviar respuestas a ctrllt (half-duplex)"
    )
    parser.add_argument(
        "-x", required=True, metavar="<dir-aralmac>",
        help="Directorio raíz del área de almacenamiento (aralmac)"
    )
    return parser.parse_args()


# =============================================================================
# GESTIÓN DEL ÁREA DE ALMACENAMIENTO (aralmac)
# =============================================================================

def crear_directorios_aralmac(dir_aralmac):
    """
    Crea el subdirectorio 'programas/' dentro de aralmac si no existe.

    Parámetros:
        dir_aralmac: ruta raíz del aralmac

    Retorna:
        Ruta del subdirectorio 'programas/'
    """
    dir_programas = os.path.join(dir_aralmac, "programas")
    os.makedirs(dir_programas, exist_ok=True)
    return dir_programas


def obtener_siguiente_id(dir_aralmac):
    """
    Lee el contador de programas y retorna el próximo ID disponible.

    Usa aralmac/programas_counter.json con formato {"next": N}.
    El contador se incrementa en cada llamada y persiste en disco para
    garantizar IDs únicos incluso si el servicio se reinicia.

    Parámetros:
        dir_aralmac: directorio raíz de aralmac

    Retorna:
        String con formato "p-XXXX" (ej. "p-0001")
    """
    ruta_counter = os.path.join(dir_aralmac, "programas_counter.json")

    if not os.path.exists(ruta_counter):
        siguiente = 1
    else:
        with open(ruta_counter, "r", encoding="utf-8") as f:
            datos = json.load(f)
        siguiente = datos.get("next", 1)

    # Persistir el nuevo valor antes de retornar (previene duplicados)
    with open(ruta_counter, "w", encoding="utf-8") as f:
        json.dump({"next": siguiente + 1}, f)

    return f"p-{siguiente:04d}"


# =============================================================================
# COMUNICACIÓN POR TUBERÍAS NOMBRADAS
# =============================================================================

def crear_y_abrir_pipes(ruta_req, ruta_res):
    """
    Crea las FIFOs si no existen y las abre sin bloquear (O_RDWR).

    Ver la explicación detallada de por qué O_RDWR en gesfich.py.
    Aquí aplicamos el mismo principio: los servicios deben poder
    arrancar en cualquier orden sin interbloqueo.

    Parámetros:
        ruta_req: FIFO de peticiones (este proceso LEERÁ de aquí)
        ruta_res: FIFO de respuestas (este proceso ESCRIBIRÁ aquí), o None

    Retorna:
        Tupla (fd_lectura, fd_escritura)
    """
    for ruta in [ruta_req, ruta_res]:
        if ruta and not os.path.exists(ruta):
            os.mkfifo(ruta)
            print(f"[gesprog] FIFO creada: {ruta}", flush=True)

    fd_lectura = os.open(ruta_req, os.O_RDWR)

    if ruta_res:
        fd_escritura = os.open(ruta_res, os.O_RDWR)
    else:
        fd_escritura = fd_lectura

    return fd_lectura, fd_escritura


def leer_mensaje(fd_lectura):
    """
    Lee un mensaje JSON de la FIFO, byte a byte hasta encontrar '\\n'.

    Mismo mecanismo que gesfich.py — ver comentario detallado allí.

    Parámetros:
        fd_lectura: descriptor del FIFO de peticiones

    Retorna:
        Diccionario Python o None si hubo error
    """
    buffer = b""
    while True:
        try:
            byte = os.read(fd_lectura, 1)
        except OSError as e:
            print(f"[gesprog] Error leyendo pipe: {e}", flush=True)
            return None

        if not byte:
            return None

        if byte == b"\n":
            break

        buffer += byte

        if len(buffer) > MSG_MAX_LEN:
            print(f"[gesprog] ADVERTENCIA: mensaje demasiado largo, descartando", flush=True)
            return None

    if not buffer:
        return None

    try:
        return json.loads(buffer.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"[gesprog] JSON inválido: {e}", flush=True)
        return None


def enviar_respuesta(fd_escritura, datos):
    """
    Serializa y envía un diccionario como JSON + '\\n' al FIFO de respuestas.

    Parámetros:
        fd_escritura: descriptor del FIFO de respuestas
        datos       : diccionario a serializar
    """
    linea = json.dumps(datos, ensure_ascii=False) + "\n"
    try:
        os.write(fd_escritura, linea.encode("utf-8"))
    except OSError as e:
        print(f"[gesprog] Error enviando respuesta: {e}", flush=True)


# =============================================================================
# OPERACIONES DEL SERVICIO
# =============================================================================

def op_guardar(dir_programas, dir_aralmac, peticion):
    """
    Registra un programa ejecutable con sus metadatos en aralmac.

    Valida que el ejecutable exista y sea ejecutable antes de guardarlo.
    Los campos 'args' y 'env' son opcionales según el enunciado.

    Parámetros:
        dir_programas: subdirectorio 'programas/' en aralmac
        dir_aralmac  : directorio raíz de aralmac (para el contador)
        peticion     : debe contener "ejecutable"; opcionalmente "args" y "env"

    Retorna:
        {"estado":"ok","id-programa":"p-XXXX"} o error
    """
    ejecutable = peticion.get("ejecutable")

    if not ejecutable:
        return {"estado": "error", "mensaje": "falta campo: ejecutable"}

    # Verificar que el ejecutable existe y tiene permisos de ejecución
    if not os.path.isfile(ejecutable) or not os.access(ejecutable, os.X_OK):
        return {"estado": "error", "mensaje": "no se pudo guardar el programa"}

    id_programa = obtener_siguiente_id(dir_aralmac)

    # Construir el objeto de metadatos según la especificación del enunciado
    metadatos = {
        "id-programa": id_programa,
        "nombre":      os.path.basename(ejecutable),  # Solo el nombre, sin la ruta
        "ejecutable":  ejecutable,
        "args":        peticion.get("args", []),       # Lista vacía si no se especifica
        "env":         peticion.get("env", [])         # Lista vacía si no se especifica
    }

    ruta_json = os.path.join(dir_programas, f"{id_programa}.json")

    try:
        with open(ruta_json, "w", encoding="utf-8") as f:
            json.dump(metadatos, f, ensure_ascii=False, indent=2)
        print(f"[gesprog] Guardado: {id_programa} → {ejecutable}", flush=True)
        return {"estado": "ok", "id-programa": id_programa}
    except OSError as e:
        print(f"[gesprog] Error guardando {id_programa}: {e}", flush=True)
        return {"estado": "error", "mensaje": "no se pudo guardar el programa"}


def op_leer(dir_programas, peticion):
    """
    Lee los metadatos de un programa por ID, o lista todos los programas.

    Modo 1 — con "id-programa": retorna el objeto de metadatos completo
    Modo 2 — sin "id-programa": retorna la lista de todos los IDs

    Esta operación es la única permitida en estado SUSPENDIDO, según
    la figura 4 del enunciado.

    Parámetros:
        dir_programas: subdirectorio 'programas/' en aralmac
        peticion     : diccionario completo de la petición

    Retorna:
        Modo 1: {"estado":"ok","programa":{...metadatos...}}
        Modo 2: {"estado":"ok","programas":["p-0001","p-0002",...]}
        Error:  {"estado":"error","mensaje":"..."}
    """
    id_programa = peticion.get("id-programa")

    if id_programa:
        # Modo 1: leer metadatos de un programa específico
        ruta_json = os.path.join(dir_programas, f"{id_programa}.json")

        if not os.path.exists(ruta_json):
            return {"estado": "error", "mensaje": "programa no encontrado"}

        try:
            with open(ruta_json, "r", encoding="utf-8") as f:
                metadatos = json.load(f)
            # La respuesta envuelve los metadatos bajo la clave "programa"
            return {"estado": "ok", "programa": metadatos}
        except (OSError, json.JSONDecodeError) as e:
            print(f"[gesprog] Error leyendo {id_programa}: {e}", flush=True)
            return {"estado": "error", "mensaje": "programa no encontrado"}

    else:
        # Modo 2: listar todos los programas registrados
        try:
            todos = [
                nombre.replace(".json", "")
                for nombre in os.listdir(dir_programas)
                if nombre.startswith("p-") and nombre.endswith(".json")
                and len(nombre) == 11  # "p-XXXX.json" = 11 caracteres
            ]
            todos.sort()
            return {"estado": "ok", "programas": todos}
        except OSError as e:
            print(f"[gesprog] Error listando programas: {e}", flush=True)
            return {"estado": "error", "mensaje": "error al listar programas"}


def op_actualizar(dir_programas, peticion):
    """
    Actualiza la ruta del ejecutable de un programa registrado.

    Según el enunciado, Actualizar recibe "id-programa" y "ruta" (nueva ruta
    del ejecutable). Actualiza el campo "ejecutable" y "nombre" en el JSON.

    Parámetros:
        dir_programas: subdirectorio 'programas/' en aralmac
        peticion     : debe contener "id-programa" y "ruta"

    Retorna:
        {"estado":"ok"} o error
    """
    id_programa  = peticion.get("id-programa")
    nueva_ruta   = peticion.get("ruta")

    if not id_programa or not nueva_ruta:
        return {"estado": "error", "mensaje": "faltan campos: id-programa, ruta"}

    ruta_json = os.path.join(dir_programas, f"{id_programa}.json")

    if not os.path.exists(ruta_json):
        return {"estado": "error", "mensaje": "programa no encontrado"}

    # Verificar que la nueva ruta del ejecutable existe
    if not os.path.isfile(nueva_ruta) or not os.access(nueva_ruta, os.X_OK):
        return {"estado": "error", "mensaje": "no se pudo actualizar el programa"}

    try:
        with open(ruta_json, "r", encoding="utf-8") as f:
            metadatos = json.load(f)

        # Actualizar la ruta y el nombre base del ejecutable
        metadatos["ejecutable"] = nueva_ruta
        metadatos["nombre"]     = os.path.basename(nueva_ruta)

        with open(ruta_json, "w", encoding="utf-8") as f:
            json.dump(metadatos, f, ensure_ascii=False, indent=2)

        print(f"[gesprog] Actualizado: {id_programa} → {nueva_ruta}", flush=True)
        return {"estado": "ok"}
    except (OSError, json.JSONDecodeError) as e:
        print(f"[gesprog] Error actualizando {id_programa}: {e}", flush=True)
        return {"estado": "error", "mensaje": "no se pudo actualizar el programa"}


def op_borrar(dir_programas, peticion):
    """
    Elimina un programa del aralmac dado su identificador.

    Parámetros:
        dir_programas: subdirectorio 'programas/' en aralmac
        peticion     : debe contener "id-programa"

    Retorna:
        {"estado":"ok"} o error
    """
    id_programa = peticion.get("id-programa")

    if not id_programa:
        return {"estado": "error", "mensaje": "faltan campos: id-programa, ruta"}

    ruta_json = os.path.join(dir_programas, f"{id_programa}.json")

    if not os.path.exists(ruta_json):
        return {"estado": "error", "mensaje": "programa no encontrado"}

    try:
        os.remove(ruta_json)
        print(f"[gesprog] Borrado: {id_programa}", flush=True)
        return {"estado": "ok"}
    except OSError as e:
        print(f"[gesprog] Error borrando {id_programa}: {e}", flush=True)
        return {"estado": "error", "mensaje": "programa no encontrado"}


# =============================================================================
# MÁQUINA DE ESTADOS Y BUCLE PRINCIPAL
# =============================================================================

def procesar_peticion(peticion, estado_actual, dir_programas, dir_aralmac):
    """
    Despacha una petición según la operación y el estado actual.

    Diferencia CRÍTICA respecto a gesfich:
      En estado SUSPENDIDO, esta función PERMITE la operación Leer.
      gesfich bloquea todos los CRUD en suspensión; gesprog solo bloquea
      Guardar, Actualizar y Borrar. Esto refleja el diagrama del enunciado
      donde "Leer" tiene una flecha propia que sale del estado Suspendido.

    Parámetros:
        peticion     : diccionario con la petición recibida
        estado_actual: estado actual de la máquina ("Corriendo" / "Suspendido")
        dir_programas: subdirectorio 'programas/' en aralmac
        dir_aralmac  : directorio raíz de aralmac

    Retorna:
        Tupla (respuesta_dict, nuevo_estado)
    """
    operacion = peticion.get("operacion", "")

    # ── Terminar: válido desde cualquier estado ───────────────────────────────
    if operacion == "Terminar":
        print(f"[gesprog] Terminando por petición...", flush=True)
        return {"estado": "ok"}, ESTADO_TERMINADO

    # ── Suspender: solo desde Corriendo ──────────────────────────────────────
    if operacion == "Suspender":
        if estado_actual == ESTADO_CORRIENDO:
            print(f"[gesprog] Corriendo → Suspendido", flush=True)
            return {"estado": "ok"}, ESTADO_SUSPENDIDO
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}, estado_actual

    # ── Reasumir: solo desde Suspendido ──────────────────────────────────────
    if operacion == "Reasumir":
        if estado_actual == ESTADO_SUSPENDIDO:
            print(f"[gesprog] Suspendido → Corriendo", flush=True)
            return {"estado": "ok"}, ESTADO_CORRIENDO
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}, estado_actual

    # ── Leer: permitida incluso en estado Suspendido ──────────────────────────
    # Este es el comportamiento específico de gesprog: se puede consultar
    # el catálogo de programas aunque el servicio esté suspendido.
    # Útil para que el ejecutor pueda lanzar programas registrados antes
    # de que gesprog se suspendiera.
    if operacion == "Leer":
        return op_leer(dir_programas, peticion), estado_actual

    # ── Demás operaciones: bloqueadas en Suspendido ───────────────────────────
    if estado_actual == ESTADO_SUSPENDIDO:
        return {"estado": "error", "mensaje": "servicio suspendido"}, estado_actual

    # ── Despacho de operaciones (solo en estado Corriendo) ────────────────────
    if operacion == "Guardar":
        respuesta = op_guardar(dir_programas, dir_aralmac, peticion)
    elif operacion == "Actualizar":
        respuesta = op_actualizar(dir_programas, peticion)
    elif operacion == "Borrar":
        respuesta = op_borrar(dir_programas, peticion)
    else:
        respuesta = {"estado": "error", "mensaje": "operacion desconocida"}

    return respuesta, estado_actual


def main():
    """
    Punto de entrada del servicio gesprog.

    Misma estructura que gesfich.main(): parsear args → crear dirs →
    abrir pipes → bucle leer/procesar/responder → limpieza.
    """
    args = parsear_argumentos()

    dir_programas = crear_directorios_aralmac(args.x)
    print(f"[gesprog] Almacenamiento en: {args.x}", flush=True)

    fd_req, fd_res = crear_y_abrir_pipes(args.p, args.c)
    print(f"[gesprog] Escuchando en: {args.p}", flush=True)

    estado = ESTADO_CORRIENDO

    # Manejador de señal para cierre limpio con SIGTERM / SIGINT (Ctrl+C)
    def manejador_senal(signum, frame):
        print(f"\n[gesprog] Señal {signum} recibida, cerrando...", flush=True)
        try:
            os.close(fd_req)
            if fd_res != fd_req:
                os.close(fd_res)
        except OSError:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, manejador_senal)
    signal.signal(signal.SIGINT,  manejador_senal)

    print(f"[gesprog] Servicio iniciado. Estado: {estado}", flush=True)

    # ── Bucle principal ───────────────────────────────────────────────────────
    while estado != ESTADO_TERMINADO:

        peticion = leer_mensaje(fd_req)

        if peticion is None:
            continue

        print(f"[gesprog] Op: {peticion.get('operacion', '?')}", flush=True)

        respuesta, estado = procesar_peticion(
            peticion, estado, dir_programas, args.x
        )

        enviar_respuesta(fd_res, respuesta)

    # ── Limpieza ──────────────────────────────────────────────────────────────
    print("[gesprog] Servicio terminado.", flush=True)
    try:
        os.close(fd_req)
        if fd_res != fd_req:
            os.close(fd_res)
    except OSError:
        pass


if __name__ == "__main__":
    main()
