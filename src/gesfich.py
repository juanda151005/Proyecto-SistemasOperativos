#!/usr/bin/env python3
"""
gesfich.py — Servicio Gestor de Ficheros

Qué hace este servicio:
  Maneja el ciclo de vida de los ficheros almacenados en el área aralmac:
  crearlos vacíos, leer su contenido, actualizar su contenido desde un archivo
  externo, y borrarlos. Es uno de los cuatro procesos del sistema de lotes.

Cómo encaja en el sistema:
  ctrllt recibe peticiones de los clientes y las reenvía a este servicio
  a través de tuberías nombradas (FIFOs). Este servicio nunca habla
  directamente con los clientes; solo con ctrllt.

  ctrllt ──[pipe_fich_req]──▶ gesfich
  ctrllt ◀──[pipe_fich_res]── gesfich

Almacenamiento (aralmac):
  aralmac/
  ├── ficheros/
  │   ├── f-0001    ← contenido real del fichero
  │   ├── f-0002
  │   └── ...
  └── ficheros_counter.json   ← {"next": 3}

Sinopsis:
  python3 gesfich.py -f <pipe_req> [-b <pipe_res>] -x <dir_aralmac>

Máquina de estados:
  Corriendo ──Suspender──▶ Suspendido ──Reasumir──▶ Corriendo
  Corriendo  ──Terminar──▶ Terminado
  Suspendido ──Terminar──▶ Terminado

Uso con el cliente (a través de ctrllt):
  Crear:     {"servicio":"gesfich","operacion":"Crear"}
  Leer uno:  {"servicio":"gesfich","operacion":"Leer","id-fichero":"f-0001"}
  Leer todo: {"servicio":"gesfich","operacion":"Leer"}
  Actualizar:{"servicio":"gesfich","operacion":"Actualizar","id-fichero":"f-0001","ruta":"/ruta"}
  Borrar:    {"servicio":"gesfich","operacion":"Borrar","id-fichero":"f-0001"}
"""

import os
import sys
import json
import argparse
import signal

# ── Constantes del protocolo (según el enunciado) ────────────────────────────
MSG_MAX_LEN = 4096  # Máximo tamaño de un mensaje JSON en bytes

# ── Identificadores de estado de la máquina de estados ───────────────────────
ESTADO_CORRIENDO  = "Corriendo"
ESTADO_SUSPENDIDO = "Suspendido"
ESTADO_TERMINADO  = "Terminado"


# =============================================================================
# ARGUMENTOS DE LÍNEA DE COMANDOS
# =============================================================================

def parsear_argumentos():
    """
    Parsea los argumentos de línea de comandos.

    Retorna:
        argparse.Namespace con:
          - f  : ruta de la FIFO de peticiones (gesfich lee de aquí)
          - b  : ruta de la FIFO de respuestas (gesfich escribe aquí); None si
                 el sistema usa una sola pipe full-duplex
          - x  : ruta del directorio aralmac
    """
    parser = argparse.ArgumentParser(
        description="gesfich — Servicio de Gestión de Ficheros del sistema de lotes"
    )
    parser.add_argument(
        "-f", required=True, metavar="<tuberia-req>",
        help="Tubería nombrada para recibir peticiones de ctrllt"
    )
    parser.add_argument(
        "-b", required=False, default=None, metavar="<tuberia-res>",
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
    Crea la estructura de directorios de aralmac si no existe.

    Parámetros:
        dir_aralmac: ruta base del directorio de almacenamiento

    Retorna:
        Ruta del subdirectorio 'ficheros/' dentro de aralmac
    """
    dir_ficheros = os.path.join(dir_aralmac, "ficheros")
    # exist_ok=True evita error si el directorio ya existe
    os.makedirs(dir_ficheros, exist_ok=True)
    return dir_ficheros


def obtener_siguiente_id(dir_aralmac):
    """
    Lee el contador de ficheros y retorna el próximo ID disponible.

    El contador persiste en aralmac/ficheros_counter.json con formato:
        {"next": 3}
    Al leerlo, se incrementa inmediatamente para que la próxima llamada
    reciba un ID diferente. Esto garantiza IDs únicos incluso si el
    sistema se reinicia (el archivo persiste en disco).

    Parámetros:
        dir_aralmac: ruta base del aralmac

    Retorna:
        String con el ID formateado, ej. "f-0001", "f-0002", ...
    """
    ruta_counter = os.path.join(dir_aralmac, "ficheros_counter.json")

    # Si no existe el contador, empezamos desde 1
    if not os.path.exists(ruta_counter):
        siguiente = 1
    else:
        with open(ruta_counter, "r", encoding="utf-8") as f:
            datos = json.load(f)
        siguiente = datos.get("next", 1)

    # Guardar el valor incrementado ANTES de retornar, para evitar duplicados
    with open(ruta_counter, "w", encoding="utf-8") as f:
        json.dump({"next": siguiente + 1}, f)

    # Formato f-XXXX: cuatro dígitos con ceros a la izquierda
    return f"f-{siguiente:04d}"


# =============================================================================
# COMUNICACIÓN POR TUBERÍAS NOMBRADAS (IPC con FIFOs)
# =============================================================================

def crear_y_abrir_pipes(ruta_req, ruta_res):
    """
    Crea las FIFOs si no existen y las abre sin bloquear el proceso.

    Por qué O_RDWR en lugar de O_RDONLY / O_WRONLY:
      En Linux, abrir un FIFO con O_RDONLY bloquea el proceso hasta que
      alguien abra el otro extremo con O_WRONLY. Si los servicios arrancan
      antes que ctrllt, se quedarían bloqueados esperando. Con O_RDWR,
      el kernel considera que hay al menos un lector y un escritor (el mismo
      proceso), por lo que no bloquea. Esto permite que los procesos arranquen
      en cualquier orden sin interbloqueo.

      Tradeoff: con O_RDWR no recibiremos EOF cuando el escritor cierre la pipe.
      Para este sistema académico, el ciclo de vida se controla con la
      operación "Terminar", no con el cierre físico de la pipe.

    Parámetros:
        ruta_req: path de la FIFO de peticiones (este servicio LEERÁ de aquí)
        ruta_res: path de la FIFO de respuestas (este servicio ESCRIBIRÁ aquí);
                  puede ser None si la pipe de petición es full-duplex

    Retorna:
        Tupla (fd_lectura, fd_escritura) — descriptores de archivo Unix
    """
    # Crear las FIFOs físicas si aún no existen en el sistema de archivos
    for ruta in [ruta_req, ruta_res]:
        if ruta and not os.path.exists(ruta):
            os.mkfifo(ruta)
            print(f"[gesfich] FIFO creada: {ruta}", flush=True)

    # Abrir pipe de peticiones con O_RDWR para no bloquearnos al arrancar
    fd_lectura = os.open(ruta_req, os.O_RDWR)

    # Si hay pipe separada de respuestas (arquitectura half-duplex), la abrimos
    if ruta_res:
        fd_escritura = os.open(ruta_res, os.O_RDWR)
    else:
        # Si no se especificó pipe de respuesta, usamos la misma (no debería
        # usarse así en Linux half-duplex, pero lo soportamos por flexibilidad)
        fd_escritura = fd_lectura

    return fd_lectura, fd_escritura


def leer_mensaje(fd_lectura):
    """
    Lee exactamente un mensaje JSON de la FIFO, byte a byte.

    Por qué leer byte a byte y no en bloques grandes:
      Un FIFO puede devolver MENOS bytes de los que pedimos si el buffer
      interno está parcialmente lleno. Si pedimos 4096 bytes y solo hay
      50, los obtenemos. Pero si concatenamos dos mensajes sin delimitador,
      el json.loads() fallaría al recibir "msg1\\nmsg2" como un bloque.
      Leyendo byte a byte hasta el '\\n', garantizamos exactamente un
      mensaje por llamada, sin importar cuántos mensajes estén en el buffer.

    Qué pasa si el mensaje llega incompleto:
      En un FIFO, el kernel garantiza que escrituras atómicas (< PIPE_BUF,
      que es 4096 bytes en Linux) llegan completas. Como MSG_MAX_LEN = 4096,
      cada mensaje llega completo en una sola escritura. No hay mensajes
      "partidos a la mitad".

    Parámetros:
        fd_lectura: descriptor del FIFO de peticiones

    Retorna:
        Diccionario Python con el mensaje parseado, o None si hubo error
    """
    buffer = b""
    while True:
        try:
            byte = os.read(fd_lectura, 1)
        except OSError as e:
            print(f"[gesfich] Error leyendo pipe: {e}", flush=True)
            return None

        if not byte:
            # EOF: el escritor cerró la pipe (raro con O_RDWR, pero posible)
            return None

        if byte == b"\n":
            # Delimitador encontrado: el mensaje está completo
            break

        buffer += byte

        # Protección contra mensajes malformados que no tengan delimitador
        if len(buffer) > MSG_MAX_LEN:
            print(f"[gesfich] ADVERTENCIA: mensaje supera {MSG_MAX_LEN} bytes, descartando", flush=True)
            return None

    if not buffer:
        # Línea vacía: ignorar silenciosamente
        return None

    try:
        return json.loads(buffer.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"[gesfich] JSON inválido: {e}", flush=True)
        return None


def enviar_respuesta(fd_escritura, datos):
    """
    Serializa un diccionario como JSON y lo escribe en la FIFO de respuestas.

    El '\\n' al final es el delimitador del protocolo: es lo que el receptor
    (ctrllt) usa para saber dónde termina este mensaje y empieza el siguiente.

    Parámetros:
        fd_escritura: descriptor del FIFO de respuestas
        datos: diccionario Python a serializar
    """
    linea = json.dumps(datos, ensure_ascii=False) + "\n"
    try:
        os.write(fd_escritura, linea.encode("utf-8"))
    except OSError as e:
        print(f"[gesfich] Error enviando respuesta: {e}", flush=True)


# =============================================================================
# OPERACIONES DEL SERVICIO (lógica de negocio)
# =============================================================================

def op_crear(dir_ficheros, dir_aralmac):
    """
    Crea un fichero vacío en aralmac y retorna su identificador.

    Parámetros:
        dir_ficheros: subdirectorio 'ficheros/' dentro de aralmac
        dir_aralmac : directorio raíz de aralmac (para el contador)

    Retorna:
        {"estado":"ok","id-fichero":"f-XXXX"} o {"estado":"error","mensaje":"..."}
    """
    id_fichero = obtener_siguiente_id(dir_aralmac)
    ruta_archivo = os.path.join(dir_ficheros, id_fichero)

    try:
        # Crear el archivo vacío; 'w' trunca si existiera (no debería con ID único)
        with open(ruta_archivo, "w", encoding="utf-8") as f:
            pass  # Archivo vacío: el contenido lo carga la operación Actualizar
        print(f"[gesfich] Creado: {id_fichero}", flush=True)
        return {"estado": "ok", "id-fichero": id_fichero}
    except OSError as e:
        print(f"[gesfich] Error creando {id_fichero}: {e}", flush=True)
        return {"estado": "error", "mensaje": "no se pudo crear el fichero"}


def op_leer(dir_ficheros, peticion):
    """
    Lee el contenido de un fichero o lista todos los ficheros registrados.

    Modo 1 — con "id-fichero": retorna el contenido del fichero específico
    Modo 2 — sin "id-fichero": retorna la lista de todos los IDs existentes

    Parámetros:
        dir_ficheros: subdirectorio 'ficheros/' en aralmac
        peticion    : diccionario completo de la petición recibida

    Retorna:
        Modo 1: {"estado":"ok","contenido":"<texto>"}
        Modo 2: {"estado":"ok","ficheros":["f-0001","f-0002",...]}
        Error:  {"estado":"error","mensaje":"..."}
    """
    id_fichero = peticion.get("id-fichero")

    if id_fichero:
        # Modo 1: leer contenido de un fichero específico por su ID
        ruta_archivo = os.path.join(dir_ficheros, id_fichero)

        if not os.path.exists(ruta_archivo):
            return {"estado": "error", "mensaje": "fichero no encontrado"}

        try:
            with open(ruta_archivo, "r", encoding="utf-8") as f:
                contenido = f.read()
            return {"estado": "ok", "contenido": contenido}
        except OSError as e:
            print(f"[gesfich] Error leyendo {id_fichero}: {e}", flush=True)
            return {"estado": "error", "mensaje": "fichero no encontrado"}

    else:
        # Modo 2: listar todos los ficheros en el directorio
        try:
            # Solo incluimos archivos con formato f-XXXX (4 dígitos)
            todos = [
                nombre for nombre in os.listdir(dir_ficheros)
                if nombre.startswith("f-") and len(nombre) == 6
                and nombre[2:].isdigit()
            ]
            todos.sort()  # Ordenar: f-0001, f-0002, ...
            return {"estado": "ok", "ficheros": todos}
        except OSError as e:
            print(f"[gesfich] Error listando ficheros: {e}", flush=True)
            return {"estado": "error", "mensaje": "error al listar ficheros"}


def op_actualizar(dir_ficheros, peticion):
    """
    Reemplaza el contenido de un fichero en aralmac con el de un archivo externo.

    El campo "ruta" apunta a un archivo en el sistema de archivos del cliente.
    Este servicio lo copia dentro del aralmac.

    Parámetros:
        dir_ficheros: subdirectorio 'ficheros/' en aralmac
        peticion    : debe contener "id-fichero" y "ruta"

    Retorna:
        {"estado":"ok"} o {"estado":"error","mensaje":"..."}
    """
    id_fichero  = peticion.get("id-fichero")
    ruta_fuente = peticion.get("ruta")

    # Validar que se proporcionaron ambos campos obligatorios
    if not id_fichero or not ruta_fuente:
        return {"estado": "error", "mensaje": "faltan campos: id-fichero, ruta"}

    ruta_destino = os.path.join(dir_ficheros, id_fichero)

    # Verificar que el fichero de destino existe en aralmac (debe haber sido creado)
    if not os.path.exists(ruta_destino):
        return {"estado": "error", "mensaje": "fichero no encontrado"}

    # Verificar que el archivo fuente existe en el sistema de archivos del sistema
    if not os.path.isfile(ruta_fuente):
        return {"estado": "error", "mensaje": "no se pudo actualizar el fichero"}

    try:
        # Copiar el contenido del archivo fuente al fichero en aralmac
        with open(ruta_fuente, "r", encoding="utf-8", errors="replace") as src:
            contenido = src.read()
        with open(ruta_destino, "w", encoding="utf-8") as dst:
            dst.write(contenido)
        print(f"[gesfich] Actualizado: {id_fichero} desde {ruta_fuente}", flush=True)
        return {"estado": "ok"}
    except OSError as e:
        print(f"[gesfich] Error actualizando {id_fichero}: {e}", flush=True)
        return {"estado": "error", "mensaje": "no se pudo actualizar el fichero"}


def op_borrar(dir_ficheros, peticion):
    """
    Elimina un fichero del aralmac dado su identificador.

    Parámetros:
        dir_ficheros: subdirectorio 'ficheros/' en aralmac
        peticion    : debe contener "id-fichero"

    Retorna:
        {"estado":"ok"} o {"estado":"error","mensaje":"..."}
    """
    id_fichero = peticion.get("id-fichero")

    if not id_fichero:
        return {"estado": "error", "mensaje": "faltan campos: id-fichero, ruta"}

    ruta_archivo = os.path.join(dir_ficheros, id_fichero)

    if not os.path.exists(ruta_archivo):
        return {"estado": "error", "mensaje": "fichero no encontrado"}

    try:
        os.remove(ruta_archivo)
        print(f"[gesfich] Borrado: {id_fichero}", flush=True)
        return {"estado": "ok"}
    except OSError as e:
        print(f"[gesfich] Error borrando {id_fichero}: {e}", flush=True)
        return {"estado": "error", "mensaje": "fichero no encontrado"}


# =============================================================================
# MÁQUINA DE ESTADOS Y BUCLE PRINCIPAL
# =============================================================================

def procesar_peticion(peticion, estado_actual, dir_ficheros, dir_aralmac):
    """
    Despacha una petición según la operación y el estado actual del servicio.

    Esta función implementa la máquina de estados del enunciado:
      - En CORRIENDO: acepta todas las operaciones
      - En SUSPENDIDO: solo acepta Reasumir y Terminar; el resto devuelve error
      - En cualquier estado: Terminar transiciona a TERMINADO

    Por qué manejar el estado aquí y no en el bucle principal:
      Separar la lógica de estados del bucle de I/O facilita probar cada
      pieza por separado. El bucle solo lee/escribe pipes; esta función
      decide qué hacer con cada mensaje.

    Parámetros:
        peticion     : diccionario con la petición recibida
        estado_actual: string con el estado actual ("Corriendo" / "Suspendido")
        dir_ficheros : subdirectorio 'ficheros/' en aralmac
        dir_aralmac  : directorio raíz de aralmac

    Retorna:
        Tupla (respuesta_dict, nuevo_estado)
          - respuesta_dict: lo que se enviará de vuelta al cliente
          - nuevo_estado: puede ser igual o diferente al estado_actual
    """
    operacion = peticion.get("operacion", "")

    # ── Terminar: válido desde cualquier estado ───────────────────────────────
    if operacion == "Terminar":
        print(f"[gesfich] Terminando por petición...", flush=True)
        return {"estado": "ok"}, ESTADO_TERMINADO

    # ── Suspender: solo desde Corriendo ──────────────────────────────────────
    if operacion == "Suspender":
        if estado_actual == ESTADO_CORRIENDO:
            print(f"[gesfich] {estado_actual} → Suspendido", flush=True)
            return {"estado": "ok"}, ESTADO_SUSPENDIDO
        else:
            # Ya está suspendido: transición inválida
            return {"estado": "error", "mensaje": "transicion invalida"}, estado_actual

    # ── Reasumir: solo desde Suspendido ──────────────────────────────────────
    if operacion == "Reasumir":
        if estado_actual == ESTADO_SUSPENDIDO:
            print(f"[gesfich] Suspendido → Corriendo", flush=True)
            return {"estado": "ok"}, ESTADO_CORRIENDO
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}, estado_actual

    # ── CRUD: bloqueado si el servicio está suspendido ────────────────────────
    # Cuando el servicio está suspendido, las operaciones de datos se rechazan.
    # El sistema debe enviar Reasumir antes de que el servicio las procese.
    if estado_actual == ESTADO_SUSPENDIDO:
        return {"estado": "error", "mensaje": "servicio suspendido"}, estado_actual

    # ── Despacho de operaciones CRUD (solo en estado Corriendo) ───────────────
    if operacion == "Crear":
        respuesta = op_crear(dir_ficheros, dir_aralmac)
    elif operacion == "Leer":
        respuesta = op_leer(dir_ficheros, peticion)
    elif operacion == "Actualizar":
        respuesta = op_actualizar(dir_ficheros, peticion)
    elif operacion == "Borrar":
        respuesta = op_borrar(dir_ficheros, peticion)
    else:
        respuesta = {"estado": "error", "mensaje": "operacion desconocida"}

    # El estado no cambia con operaciones CRUD
    return respuesta, estado_actual


def main():
    """
    Punto de entrada del servicio gesfich.

    Flujo:
      1. Parsear argumentos de línea de comandos
      2. Crear estructura de directorios en aralmac
      3. Crear y abrir las FIFOs (sin bloquear)
      4. Registrar manejadores de señal para cierre limpio
      5. Bucle principal: leer → procesar → responder
      6. Al recibir Terminar o señal, cerrar pipes y salir
    """
    args = parsear_argumentos()

    # Crear directorios si no existen
    dir_ficheros = crear_directorios_aralmac(args.x)
    print(f"[gesfich] Almacenamiento en: {args.x}", flush=True)

    # Abrir las FIFOs sin bloquear (O_RDWR)
    fd_req, fd_res = crear_y_abrir_pipes(args.f, args.b)
    print(f"[gesfich] Escuchando en: {args.f}", flush=True)

    # Estado inicial: el servicio arranca en Corriendo
    estado = ESTADO_CORRIENDO

    # ── Manejador de señales para cierre limpio ───────────────────────────────
    # Necesario para que Ctrl+C o kill no dejen las FIFOs abiertas o en
    # estado inconsistente. Al recibir la señal, cerramos los fds antes de salir.
    def manejador_senal(signum, frame):
        print(f"\n[gesfich] Señal {signum} recibida, cerrando...", flush=True)
        try:
            os.close(fd_req)
            if fd_res != fd_req:
                os.close(fd_res)
        except OSError:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, manejador_senal)
    signal.signal(signal.SIGINT,  manejador_senal)

    print(f"[gesfich] Servicio iniciado. Estado: {estado}", flush=True)

    # ── Bucle principal de atención de peticiones ─────────────────────────────
    # El bucle se ejecuta mientras el estado no sea Terminado.
    # Cada iteración: leer una petición, procesarla, enviar respuesta.
    while estado != ESTADO_TERMINADO:

        # leer_mensaje() bloquea aquí hasta que ctrllt envíe algo
        peticion = leer_mensaje(fd_req)

        if peticion is None:
            # Pipe cerrada o mensaje inválido: re-intentar en la siguiente iteración
            # Con O_RDWR esto no debería pasar, pero lo manejamos por robustez
            continue

        print(f"[gesfich] Op: {peticion.get('operacion', '?')}", flush=True)

        # Procesar la petición; puede cambiar el estado del servicio
        respuesta, estado = procesar_peticion(
            peticion, estado, dir_ficheros, args.x
        )

        # Enviar la respuesta a ctrllt (que la reenviará al cliente original)
        enviar_respuesta(fd_res, respuesta)

    # ── Limpieza final ────────────────────────────────────────────────────────
    print("[gesfich] Servicio terminado.", flush=True)
    try:
        os.close(fd_req)
        if fd_res != fd_req:
            os.close(fd_res)
    except OSError:
        pass


if __name__ == "__main__":
    main()
