#!/usr/bin/env python3
"""
ejecutor.py — Servicio Ejecutor de Procesos de Lotes

Qué hace este servicio:
  Lanza programas registrados en aralmac como procesos de lotes.
  Un proceso de lotes combina un programa (p-XXXX) con ficheros opcionales
  para stdin, stdout y stderr (f-XXXX). El ejecutor gestiona el ciclo de
  vida de estos procesos: lanzarlos, consultar su estado, matarlos,
  suspender/reanudar todos y parar el servicio ordenadamente.

Cómo encaja en el sistema:
  ctrllt ──[pipe_ejec_req]──▶ ejecutor (recibe peticiones)
  ctrllt ◀──[pipe_ejec_res]── ejecutor (envía respuestas)

  El ejecutor lee directamente del aralmac para obtener los metadatos
  del programa (aralmac/programas/p-XXXX.json) y los ficheros de I/O
  (aralmac/ficheros/f-XXXX). NO pasa por gesprog ni gesfich en tiempo
  de ejecución.

Almacenamiento en memoria (no persiste al reiniciar):
  Los procesos lanzados se guardan en un diccionario:
    procesos[id_ejecucion] = {
      "id-ejecucion":  "e-0001",
      "id-programa":   "p-0001",
      "proceso-estado":"Ejecutando",  # | "Suspendido" | "Terminado"
      "pid":           12345,
      "codigo-salida": None           # int cuando termine
    }

Sinopsis:
  python3 ejecutor.py -e <pipe_req> [-d <pipe_res>] -x <dir_aralmac>

Máquina de estados del SERVICIO:
  inicio → Ejecutar ──Suspender──▶ Suspendidos ──Reasumir──▶ Ejecutar
           Ejecutar ──Parar──────▶ Parar ──(procesos=0)──▶ Terminar
           Suspendidos ──Parar───▶ Parar

  - Ejecutar:    estado normal; acepta nuevas ejecuciones
  - Suspendidos: todos los procesos activos reciben SIGSTOP; no acepta nuevos
  - Parar:       no acepta nuevas ejecuciones; termina cuando todos los procesos acaben
  - Terminar:    estado final; el proceso sale

Estados de cada PROCESO individual (proceso-estado en la respuesta):
  "Ejecutando"  → proceso corriendo (SIGSTOP no enviado)
  "Suspendido"  → proceso detenido con SIGSTOP
  "Terminado"   → proceso finalizó (exitCode disponible en "codigo-salida")
"""

import os
import sys
import json
import argparse
import signal
import subprocess
import threading

# ── Constantes del protocolo ──────────────────────────────────────────────────
MSG_MAX_LEN = 4096  # Máximo tamaño de mensaje JSON en bytes

# ── Estados de la máquina de estados del SERVICIO ────────────────────────────
SERVICIO_EJECUTAR     = "Ejecutar"
SERVICIO_SUSPENDIDOS  = "Suspendidos"
SERVICIO_PARAR        = "Parar"
SERVICIO_TERMINADO    = "Terminado"

# ── Estados de cada proceso individual ───────────────────────────────────────
PROC_EJECUTANDO = "Ejecutando"
PROC_SUSPENDIDO = "Suspendido"
PROC_TERMINADO  = "Terminado"


# =============================================================================
# ARGUMENTOS DE LÍNEA DE COMANDOS
# =============================================================================

def parsear_argumentos():
    """
    Parsea los argumentos de línea de comandos según la sinopsis del enunciado.

    Retorna:
        argparse.Namespace con:
          - e  : ruta FIFO de peticiones
          - d  : ruta FIFO de respuestas (half-duplex, opcional)
          - x  : ruta del directorio aralmac
    """
    parser = argparse.ArgumentParser(
        description="ejecutor — Servicio Ejecutor de Procesos de Lotes"
    )
    parser.add_argument(
        "-e", required=True, metavar="<tuberia-req>",
        help="Tubería nombrada para recibir peticiones de ctrllt"
    )
    parser.add_argument(
        "-d", required=False, default=None, metavar="<tuberia-res>",
        help="Tubería nombrada para enviar respuestas a ctrllt (half-duplex)"
    )
    parser.add_argument(
        "-x", required=True, metavar="<dir-aralmac>",
        help="Directorio raíz del área de almacenamiento (aralmac)"
    )
    return parser.parse_args()


# =============================================================================
# COMUNICACIÓN POR TUBERÍAS NOMBRADAS
# =============================================================================

def crear_y_abrir_pipes(ruta_req, ruta_res):
    """
    Crea y abre las FIFOs sin bloquear (O_RDWR). Ver gesfich.py para la
    explicación detallada de por qué O_RDWR es necesario aquí.

    Parámetros:
        ruta_req: FIFO de peticiones (este proceso LEERÁ de aquí)
        ruta_res: FIFO de respuestas (este proceso ESCRIBIRÁ aquí), o None

    Retorna:
        Tupla (fd_lectura, fd_escritura)
    """
    for ruta in [ruta_req, ruta_res]:
        if ruta and not os.path.exists(ruta):
            os.mkfifo(ruta)
            print(f"[ejecutor] FIFO creada: {ruta}", flush=True)

    fd_lectura = os.open(ruta_req, os.O_RDWR)

    if ruta_res:
        fd_escritura = os.open(ruta_res, os.O_RDWR)
    else:
        fd_escritura = fd_lectura

    return fd_lectura, fd_escritura


def leer_mensaje(fd_lectura):
    """
    Lee un mensaje JSON de la FIFO, byte a byte hasta encontrar '\\n'.
    Mismo mecanismo que gesfich.py.

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
            print(f"[ejecutor] Error leyendo pipe: {e}", flush=True)
            return None

        if not byte:
            return None

        if byte == b"\n":
            break

        buffer += byte

        if len(buffer) > MSG_MAX_LEN:
            print(f"[ejecutor] ADVERTENCIA: mensaje demasiado largo, descartando", flush=True)
            return None

    if not buffer:
        return None

    try:
        return json.loads(buffer.decode("utf-8"))
    except json.JSONDecodeError as e:
        print(f"[ejecutor] JSON inválido: {e}", flush=True)
        return None


def enviar_respuesta(fd_escritura, datos):
    """
    Serializa y envía un diccionario como JSON + '\\n'.

    Parámetros:
        fd_escritura: descriptor del FIFO de respuestas
        datos       : diccionario a serializar
    """
    linea = json.dumps(datos, ensure_ascii=False) + "\n"
    try:
        os.write(fd_escritura, linea.encode("utf-8"))
    except OSError as e:
        print(f"[ejecutor] Error enviando respuesta: {e}", flush=True)


# =============================================================================
# GESTIÓN DEL ARALMAC (lectura directa de metadatos)
# =============================================================================

def leer_metadatos_programa(dir_aralmac, id_programa):
    """
    Lee el JSON de metadatos de un programa desde aralmac.

    El ejecutor accede DIRECTAMENTE al aralmac para obtener los datos
    del programa, sin pasar por gesprog. Esto es porque ejecutor tiene
    su propia opción -x con la ruta del aralmac y necesita leer los
    metadatos en el momento de lanzar el proceso.

    Parámetros:
        dir_aralmac: ruta raíz del aralmac
        id_programa: identificador tipo "p-XXXX"

    Retorna:
        Diccionario con los metadatos del programa, o None si no existe
    """
    ruta_json = os.path.join(dir_aralmac, "programas", f"{id_programa}.json")
    if not os.path.exists(ruta_json):
        return None
    try:
        with open(ruta_json, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def obtener_ruta_fichero(dir_aralmac, id_fichero):
    """
    Retorna la ruta del archivo físico de un fichero en aralmac.

    Parámetros:
        dir_aralmac: ruta raíz del aralmac
        id_fichero : identificador tipo "f-XXXX"

    Retorna:
        Ruta completa si existe, None si no
    """
    ruta = os.path.join(dir_aralmac, "ficheros", id_fichero)
    return ruta if os.path.exists(ruta) else None


# =============================================================================
# GESTIÓN DE IDS DE EJECUCIÓN Y REGISTRO DE PROCESOS
# =============================================================================

def obtener_siguiente_id_ejecucion(dir_aralmac):
    """
    Genera el próximo ID de ejecución con formato e-XXXX.

    Usa aralmac/ejecuciones_counter.json para persistir el contador.

    Parámetros:
        dir_aralmac: ruta raíz del aralmac

    Retorna:
        String "e-XXXX"
    """
    ruta_counter = os.path.join(dir_aralmac, "ejecuciones_counter.json")

    if not os.path.exists(ruta_counter):
        siguiente = 1
    else:
        with open(ruta_counter, "r", encoding="utf-8") as f:
            datos = json.load(f)
        siguiente = datos.get("next", 1)

    with open(ruta_counter, "w", encoding="utf-8") as f:
        json.dump({"next": siguiente + 1}, f)

    return f"e-{siguiente:04d}"


# =============================================================================
# MONITOR DE PROCESOS (hilo por proceso lanzado)
# =============================================================================

def hilo_monitor_proceso(id_ejecucion, popen_obj, procesos, lock_procesos,
                          dir_aralmac, id_stdout, id_stderr, estado_servicio_ref):
    """
    Hilo que espera a que un proceso hijo termine y actualiza su registro.

    Por qué se usa un hilo aquí:
      popen_obj.wait() es BLOQUEANTE: congela el hilo que lo llama hasta
      que el proceso hijo termine. Si lo llamáramos en el hilo principal,
      el servicio quedaría bloqueado sin poder atender nuevas peticiones
      mientras el proceso corre. Con un hilo separado por proceso, el
      hilo principal queda libre para seguir leyendo peticiones de la pipe.

      Alternativa descartada: usar subprocess.poll() en un bucle con sleep().
      Es menos eficiente (CPU innecesaria) y agrega latencia para detectar
      el fin del proceso.

    Qué hace cuando el proceso termina:
      1. Registra el código de salida en el diccionario 'procesos'
      2. Si había stdout/stderr capturados como ficheros, los datos ya
         están escritos porque Popen redirigió directamente al archivo
      3. Si el servicio está en estado Parar y no quedan procesos activos,
         señala que el servicio debe terminar

    Parámetros:
        id_ejecucion       : identificador "e-XXXX" del proceso lanzado
        popen_obj          : objeto subprocess.Popen del proceso hijo
        procesos           : diccionario compartido de todos los procesos
        lock_procesos      : threading.Lock para acceso seguro al diccionario
        dir_aralmac        : ruta del aralmac (para cerrar ficheros de salida)
        id_stdout          : id-fichero del stdout (o None)
        id_stderr          : id-fichero del stderr (o None)
        estado_servicio_ref: lista de 1 elemento [estado] para comunicar
                             al hilo principal que debe terminar (mutable ref)
    """
    # Esperar a que el proceso hijo termine (bloqueante, pero en hilo separado)
    codigo_salida = popen_obj.wait()

    # Cerrar los file handles de stdout/stderr que quedaron abiertos en el padre.
    # Por qué aquí y no en op_ejecutar: necesitamos que el proceso hijo termine
    # antes de cerrarlos para garantizar que todos los datos están en disco.
    # popen_obj.stdout es None cuando se pasó un file object (no PIPE), así que
    # recuperamos los file handles del diccionario 'procesos' y los cerramos aquí.
    with lock_procesos:
        if id_ejecucion in procesos:
            for clave in ("stdout_fh", "stderr_fh"):
                fh = procesos[id_ejecucion].get(clave)
                if fh:
                    try:
                        fh.flush()
                        fh.close()
                    except OSError:
                        pass
                    procesos[id_ejecucion][clave] = None

    # Actualizar el registro del proceso de forma segura (varios hilos pueden
    # modificar 'procesos' simultáneamente si hay múltiples procesos corriendo)
    with lock_procesos:
        if id_ejecucion in procesos:
            procesos[id_ejecucion]["proceso-estado"] = PROC_TERMINADO
            procesos[id_ejecucion]["codigo-salida"]  = codigo_salida
            procesos[id_ejecucion]["popen"]          = None  # Liberar referencia

    print(f"[ejecutor] Proceso {id_ejecucion} terminó con código {codigo_salida}", flush=True)

    # Si el servicio está en estado Parar, verificar si ya no quedan activos
    with lock_procesos:
        estado_actual = estado_servicio_ref[0]
        if estado_actual == SERVICIO_PARAR:
            activos = [
                p for p in procesos.values()
                if p["proceso-estado"] in (PROC_EJECUTANDO, PROC_SUSPENDIDO)
            ]
            if not activos:
                # No quedan procesos: el servicio puede terminar
                print("[ejecutor] No quedan procesos activos. Terminando servicio...", flush=True)
                estado_servicio_ref[0] = SERVICIO_TERMINADO


# =============================================================================
# OPERACIONES DEL SERVICIO
# =============================================================================

def op_ejecutar(peticion, dir_aralmac, procesos, lock_procesos, estado_servicio_ref):
    """
    Lanza un programa de lotes como proceso hijo del sistema operativo.

    Flujo interno:
      1. Validar id-programa
      2. Leer metadatos del programa desde aralmac/programas/p-XXXX.json
      3. Si se especificó stdin (f-XXXX), abrir el archivo como stdin del proceso
      4. Si se especificó stdout (f-XXXX), abrir el archivo como stdout del proceso
      5. Si se especificó stderr (f-XXXX), abrir el archivo como stderr del proceso
      6. Lanzar el proceso con subprocess.Popen
         - Popen hace internamente fork() + exec() en Linux
         - La redirección de stdin/stdout/stderr se hace con dup2() internamente
      7. Registrar el proceso en el diccionario 'procesos'
      8. Lanzar un hilo monitor que espera a que el proceso termine
      9. Retornar el id-ejecucion

    Parámetros:
        peticion            : debe contener "id-programa"; opcionalmente "stdin", "stdout", "stderr"
        dir_aralmac         : ruta del aralmac
        procesos            : diccionario compartido de procesos
        lock_procesos       : lock para acceso seguro al diccionario
        estado_servicio_ref : referencia al estado del servicio

    Retorna:
        {"estado":"ok","id-ejecucion":"e-XXXX"} o error
    """
    id_programa = peticion.get("id-programa")
    if not id_programa:
        return {"estado": "error", "mensaje": "falta campo: id-programa"}

    # Leer metadatos del programa directamente del aralmac
    metadatos = leer_metadatos_programa(dir_aralmac, id_programa)
    if not metadatos:
        return {"estado": "error", "mensaje": "falta campo: id-programa"}

    ejecutable = metadatos.get("ejecutable", "")
    args_prog  = metadatos.get("args", [])
    env_lista  = metadatos.get("env", [])

    # Verificar que el ejecutable todavía existe (puede haber sido borrado)
    if not os.path.isfile(ejecutable) or not os.access(ejecutable, os.X_OK):
        return {"estado": "error", "mensaje": "no se pudo ejecutar el programa"}

    # ── Construir el entorno de ejecución ─────────────────────────────────────
    # env_lista es ["CLAVE=VALOR", "OTRA=X"]; convertir a dict para Popen
    if env_lista:
        env_dict = dict(os.environ)  # Partir del entorno actual
        for par in env_lista:
            if "=" in par:
                clave, valor = par.split("=", 1)
                env_dict[clave] = valor
    else:
        env_dict = None  # None = heredar el entorno del proceso padre

    # ── Resolver ficheros de I/O en aralmac ───────────────────────────────────
    # Los campos stdin/stdout/stderr son IDs de ficheros (f-XXXX), no rutas.
    # Si no se especifican, el proceso hereda los descriptores del servicio.
    id_stdin  = peticion.get("stdin")
    id_stdout = peticion.get("stdout")
    id_stderr = peticion.get("stderr")

    # Abrir los ficheros físicos correspondientes
    stdin_fh  = None
    stdout_fh = None
    stderr_fh = None

    try:
        if id_stdin:
            ruta = obtener_ruta_fichero(dir_aralmac, id_stdin)
            if not ruta:
                return {"estado": "error", "mensaje": "no se pudo ejecutar el programa"}
            stdin_fh = open(ruta, "r", encoding="utf-8")

        if id_stdout:
            ruta = obtener_ruta_fichero(dir_aralmac, id_stdout)
            if not ruta:
                if stdin_fh: stdin_fh.close()
                return {"estado": "error", "mensaje": "no se pudo ejecutar el programa"}
            stdout_fh = open(ruta, "w", encoding="utf-8")

        if id_stderr:
            ruta = obtener_ruta_fichero(dir_aralmac, id_stderr)
            if not ruta:
                if stdin_fh:  stdin_fh.close()
                if stdout_fh: stdout_fh.close()
                return {"estado": "error", "mensaje": "no se pudo ejecutar el programa"}
            stderr_fh = open(ruta, "w", encoding="utf-8")

    except OSError as e:
        print(f"[ejecutor] Error abriendo ficheros de I/O: {e}", flush=True)
        for fh in [stdin_fh, stdout_fh, stderr_fh]:
            if fh: fh.close()
        return {"estado": "error", "mensaje": "no se pudo ejecutar el programa"}

    # ── Construir el comando completo: [ejecutable] + argumentos ──────────────
    comando = [ejecutable] + args_prog

    try:
        # subprocess.Popen lanza el proceso hijo sin bloquear este proceso.
        # En Linux hace internamente: fork() para crear proceso hijo,
        # luego exec() para cargar el ejecutable.
        # La redirección de stdin/stdout/stderr se hace con dup2() en el hijo,
        # antes de exec(), para conectar los descriptores a los archivos abiertos.
        popen_obj = subprocess.Popen(
            comando,
            stdin=stdin_fh   if stdin_fh  else None,
            stdout=stdout_fh if stdout_fh else None,
            stderr=stderr_fh if stderr_fh else None,
            env=env_dict,
            close_fds=True   # Cerrar otros fds heredados en el proceso hijo
        )
    except (OSError, subprocess.SubprocessError) as e:
        print(f"[ejecutor] Error lanzando proceso: {e}", flush=True)
        for fh in [stdin_fh, stdout_fh, stderr_fh]:
            if fh: fh.close()
        return {"estado": "error", "mensaje": "no se pudo ejecutar el programa"}

    # Cerrar los file handles en el proceso padre: Popen ya los duplicó
    # con dup2() en el hijo. Si no los cerramos aquí, tenemos una referencia
    # extra que impide detectar el fin del proceso hijo por EOF en las pipes.
    if stdin_fh:  stdin_fh.close()

    # Generar el ID de ejecución para este proceso
    id_ejecucion = obtener_siguiente_id_ejecucion(dir_aralmac)

    # Registrar el proceso en el diccionario compartido
    registro = {
        "id-ejecucion":  id_ejecucion,
        "id-programa":   id_programa,
        "proceso-estado": PROC_EJECUTANDO,
        "pid":            popen_obj.pid,
        "codigo-salida":  None,
        "popen":          popen_obj,   # Referencia al objeto Popen (para kill, etc.)
        "stdout_fh":      stdout_fh,   # Guardar para flush al terminar
        "stderr_fh":      stderr_fh
    }

    with lock_procesos:
        procesos[id_ejecucion] = registro

    # ── Lanzar el hilo monitor ────────────────────────────────────────────────
    # El hilo monitor llama a popen_obj.wait() de forma bloqueante.
    # Cuando el proceso hijo termine, el hilo actualiza el registro y
    # verifica si el servicio debe transicionar a Terminado (si está en Parar).
    # daemon=True significa que este hilo no impedirá que Python salga si
    # el hilo principal termina (cleanup automático).
    hilo = threading.Thread(
        target=hilo_monitor_proceso,
        args=(id_ejecucion, popen_obj, procesos, lock_procesos,
              dir_aralmac, id_stdout, id_stderr, estado_servicio_ref),
        daemon=True
    )
    hilo.start()

    print(f"[ejecutor] Lanzado: {id_ejecucion} (PID={popen_obj.pid}, prog={id_programa})", flush=True)
    return {"estado": "ok", "id-ejecucion": id_ejecucion}


def op_estado(peticion, procesos, lock_procesos):
    """
    Consulta el estado de un proceso específico o de todos los procesos.

    Modo 1 — con "id-ejecucion": estado de un proceso específico
    Modo 2 — sin "id-ejecucion": lista todos los procesos con su estado

    Parámetros:
        peticion     : puede contener "id-ejecucion" o estar vacío
        procesos     : diccionario compartido de procesos
        lock_procesos: lock para acceso seguro

    Retorna:
        Modo 1: objeto con id-ejecucion, id-programa, proceso-estado, y
                codigo-salida (solo si Terminado)
        Modo 2: {"estado":"ok","procesos":[...lista de objetos...]}
    """
    id_ejecucion = peticion.get("id-ejecucion")

    with lock_procesos:
        if id_ejecucion:
            # Modo 1: un proceso específico
            if id_ejecucion not in procesos:
                return {"estado": "error", "mensaje": "proceso no encontrado"}

            p = procesos[id_ejecucion]
            respuesta = {
                "estado":          "ok",
                "id-ejecucion":    p["id-ejecucion"],
                "id-programa":     p["id-programa"],
                "proceso-estado":  p["proceso-estado"]
            }
            # codigo-salida solo aparece cuando el proceso ha terminado
            if p["proceso-estado"] == PROC_TERMINADO:
                respuesta["codigo-salida"] = p["codigo-salida"]

            return respuesta

        else:
            # Modo 2: todos los procesos
            lista = []
            for p in procesos.values():
                entrada = {
                    "id-ejecucion":   p["id-ejecucion"],
                    "id-programa":    p["id-programa"],
                    "proceso-estado": p["proceso-estado"]
                }
                if p["proceso-estado"] == PROC_TERMINADO:
                    entrada["codigo-salida"] = p["codigo-salida"]
                lista.append(entrada)

            return {"estado": "ok", "procesos": lista}


def op_matar(peticion, procesos, lock_procesos):
    """
    Termina forzosamente un proceso de lotes enviándole SIGKILL.

    SIGKILL no puede ser ignorado ni capturado por el proceso hijo.
    El proceso termina inmediatamente; el hilo monitor detectará esto
    cuando popen_obj.wait() retorne y actualizará el estado a Terminado.

    Parámetros:
        peticion     : debe contener "id-ejecucion"
        procesos     : diccionario compartido
        lock_procesos: lock para acceso seguro

    Retorna:
        {"estado":"ok"} o error
    """
    id_ejecucion = peticion.get("id-ejecucion")

    if not id_ejecucion:
        return {"estado": "error", "mensaje": "falta campo: id-ejecucion"}

    with lock_procesos:
        if id_ejecucion not in procesos:
            return {"estado": "error", "mensaje": "proceso no encontrado"}

        p = procesos[id_ejecucion]

        if p["proceso-estado"] == PROC_TERMINADO:
            return {"estado": "error", "mensaje": "proceso no encontrado o ya terminado"}

        if p["popen"] is None:
            return {"estado": "error", "mensaje": "proceso no encontrado o ya terminado"}

        try:
            # kill() envía una señal al proceso. SIGKILL = terminación inmediata.
            # El hilo monitor detectará el fin y actualizará el estado.
            p["popen"].kill()  # Equivalente a os.kill(pid, signal.SIGKILL)
            print(f"[ejecutor] SIGKILL enviado a {id_ejecucion} (PID={p['pid']})", flush=True)
        except OSError as e:
            print(f"[ejecutor] Error matando {id_ejecucion}: {e}", flush=True)
            return {"estado": "error", "mensaje": "proceso no encontrado o ya terminado"}

    return {"estado": "ok"}


def op_suspender_servicio(procesos, lock_procesos):
    """
    Suspende todos los procesos activos enviando SIGSTOP a cada uno.

    SIGSTOP detiene el proceso: deja de recibir tiempo de CPU pero su estado
    en memoria se conserva. Se reanuda con SIGCONT. Esta es la suspensión
    del SERVICIO (todos los procesos), distinta de suspender uno específico.

    Parámetros:
        procesos     : diccionario compartido
        lock_procesos: lock para acceso seguro

    Retorna:
        {"estado":"ok"} siempre (errores individuales se loguean pero no fallan)
    """
    with lock_procesos:
        for id_ejec, p in procesos.items():
            if p["proceso-estado"] == PROC_EJECUTANDO and p["popen"]:
                try:
                    os.kill(p["pid"], signal.SIGSTOP)
                    p["proceso-estado"] = PROC_SUSPENDIDO
                    print(f"[ejecutor] SIGSTOP → {id_ejec} (PID={p['pid']})", flush=True)
                except OSError as e:
                    print(f"[ejecutor] Error suspendiendo {id_ejec}: {e}", flush=True)

    return {"estado": "ok"}


def op_reasumir_servicio(procesos, lock_procesos):
    """
    Reanuda todos los procesos suspendidos enviando SIGCONT a cada uno.

    Parámetros:
        procesos     : diccionario compartido
        lock_procesos: lock para acceso seguro

    Retorna:
        {"estado":"ok"}
    """
    with lock_procesos:
        for id_ejec, p in procesos.items():
            if p["proceso-estado"] == PROC_SUSPENDIDO and p["popen"]:
                try:
                    os.kill(p["pid"], signal.SIGCONT)
                    p["proceso-estado"] = PROC_EJECUTANDO
                    print(f"[ejecutor] SIGCONT → {id_ejec} (PID={p['pid']})", flush=True)
                except OSError as e:
                    print(f"[ejecutor] Error reanudando {id_ejec}: {e}", flush=True)

    return {"estado": "ok"}


# =============================================================================
# MÁQUINA DE ESTADOS Y BUCLE PRINCIPAL
# =============================================================================

def procesar_peticion(peticion, estado_servicio_ref, procesos, lock_procesos, dir_aralmac):
    """
    Despacha una petición según la operación y el estado actual del servicio.

    La máquina de estados del ejecutor es más compleja que la de gesfich/gesprog:
      - Tiene 4 estados en lugar de 3
      - En "Parar" no acepta Ejecutar pero sí Estado/Matar
      - La transición a Terminado puede ocurrir desde el hilo monitor (cuando
        todos los procesos terminan en estado Parar), no solo desde aquí

    Por qué estado_servicio_ref es una lista en vez de un string:
      En Python, los strings son inmutables y se pasan por valor. Si el hilo
      monitor necesita cambiar el estado a Terminado, necesita una referencia
      mutable. Una lista de un elemento [estado] permite modificar el contenido
      sin cambiar la referencia, y la modificación es visible en todos los
      hilos que tengan la misma lista.

    Parámetros:
        peticion            : diccionario con la petición recibida
        estado_servicio_ref : lista [estado_actual] — mutable para que el hilo
                              monitor pueda cambiar el estado
        procesos            : diccionario compartido de procesos activos
        lock_procesos       : threading.Lock para acceso seguro
        dir_aralmac         : ruta del aralmac

    Retorna:
        Diccionario con la respuesta a enviar (el estado se modifica en la ref)
    """
    operacion = peticion.get("operacion", "")
    estado_actual = estado_servicio_ref[0]

    # ── Parar ─────────────────────────────────────────────────────────────────
    if operacion == "Parar":
        if estado_actual in (SERVICIO_EJECUTAR, SERVICIO_SUSPENDIDOS):
            estado_servicio_ref[0] = SERVICIO_PARAR
            print(f"[ejecutor] Estado → Parar", flush=True)

            # Verificar si ya no hay procesos activos (caso: ningún proceso lanzado)
            with lock_procesos:
                activos = [
                    p for p in procesos.values()
                    if p["proceso-estado"] in (PROC_EJECUTANDO, PROC_SUSPENDIDO)
                ]
            if not activos:
                estado_servicio_ref[0] = SERVICIO_TERMINADO
                print("[ejecutor] Sin procesos activos. Terminando inmediatamente.", flush=True)

            return {"estado": "ok"}
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}

    # ── Suspender el servicio (todos los procesos) ────────────────────────────
    if operacion == "Suspender":
        if estado_actual == SERVICIO_EJECUTAR:
            respuesta = op_suspender_servicio(procesos, lock_procesos)
            estado_servicio_ref[0] = SERVICIO_SUSPENDIDOS
            print(f"[ejecutor] Estado → Suspendidos", flush=True)
            return respuesta
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}

    # ── Reasumir el servicio ──────────────────────────────────────────────────
    if operacion == "Reasumir":
        if estado_actual == SERVICIO_SUSPENDIDOS:
            respuesta = op_reasumir_servicio(procesos, lock_procesos)
            estado_servicio_ref[0] = SERVICIO_EJECUTAR
            print(f"[ejecutor] Estado → Ejecutar", flush=True)
            return respuesta
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}

    # ── Estado (consulta): disponible en todos los estados activos ────────────
    if operacion == "Estado":
        return op_estado(peticion, procesos, lock_procesos)

    # ── Matar: disponible en Ejecutar, Suspendidos y Parar ───────────────────
    if operacion == "Matar":
        if estado_actual in (SERVICIO_EJECUTAR, SERVICIO_SUSPENDIDOS, SERVICIO_PARAR):
            return op_matar(peticion, procesos, lock_procesos)
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}

    # ── Ejecutar: solo en estado Ejecutar ─────────────────────────────────────
    if operacion == "Ejecutar":
        if estado_actual == SERVICIO_EJECUTAR:
            return op_ejecutar(peticion, dir_aralmac, procesos, lock_procesos, estado_servicio_ref)
        elif estado_actual == SERVICIO_SUSPENDIDOS:
            return {"estado": "error", "mensaje": "servicio suspendido"}
        elif estado_actual == SERVICIO_PARAR:
            return {"estado": "error", "mensaje": "servicio parando"}
        else:
            return {"estado": "error", "mensaje": "transicion invalida"}

    return {"estado": "error", "mensaje": "operacion desconocida"}


def main():
    """
    Punto de entrada del servicio ejecutor.

    Estructura similar a gesfich/gesprog, pero con:
      - Un diccionario de procesos y un Lock para acceso concurrente
        (los hilos monitores modifican el diccionario simultáneamente)
      - El estado del servicio es una lista mutable [estado] para que
        los hilos monitores puedan transicionar a Terminado cuando no
        queden procesos en estado Parar
    """
    args = parsear_argumentos()

    # Crear directorio de ejecuciones si no existe
    os.makedirs(args.x, exist_ok=True)
    print(f"[ejecutor] Almacenamiento en: {args.x}", flush=True)

    fd_req, fd_res = crear_y_abrir_pipes(args.e, args.d)
    print(f"[ejecutor] Escuchando en: {args.e}", flush=True)

    # ── Estructuras compartidas entre el hilo principal y los hilos monitor ───
    # procesos: diccionario {id_ejecucion → registro} de todos los procesos lanzados
    procesos = {}

    # lock_procesos: protege el acceso concurrente al diccionario.
    # Por qué se necesita: el hilo principal escribe (al lanzar un proceso)
    # y los hilos monitor también escriben (al actualizar el estado cuando
    # el proceso termina). Sin lock, podría ocurrir una escritura parcial.
    lock_procesos = threading.Lock()

    # estado_servicio_ref: lista mutable de un elemento para que los hilos
    # monitor puedan cambiar el estado (ver comentario en procesar_peticion)
    estado_servicio_ref = [SERVICIO_EJECUTAR]

    # Manejador de señal para cierre limpio
    def manejador_senal(signum, frame):
        print(f"\n[ejecutor] Señal {signum} recibida, cerrando...", flush=True)
        try:
            os.close(fd_req)
            if fd_res != fd_req:
                os.close(fd_res)
        except OSError:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, manejador_senal)
    signal.signal(signal.SIGINT,  manejador_senal)

    print(f"[ejecutor] Servicio iniciado. Estado: {SERVICIO_EJECUTAR}", flush=True)

    # ── Bucle principal ───────────────────────────────────────────────────────
    while estado_servicio_ref[0] != SERVICIO_TERMINADO:

        peticion = leer_mensaje(fd_req)

        if peticion is None:
            continue

        print(f"[ejecutor] Op: {peticion.get('operacion', '?')}", flush=True)

        # procesar_peticion puede modificar estado_servicio_ref[0] directamente
        # (transiciones Parar→Terminado también pueden venir del hilo monitor)
        respuesta = procesar_peticion(
            peticion, estado_servicio_ref, procesos, lock_procesos, args.x
        )

        enviar_respuesta(fd_res, respuesta)

    # ── Limpieza final ────────────────────────────────────────────────────────
    print("[ejecutor] Servicio terminado.", flush=True)
    try:
        os.close(fd_req)
        if fd_res != fd_req:
            os.close(fd_res)
    except OSError:
        pass


if __name__ == "__main__":
    main()
