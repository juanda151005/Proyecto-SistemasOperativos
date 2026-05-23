# Diseño del Sistema: Ejecutor de Lotes

**Autor:** Juan David Velásquez Restrepo  
**Fecha:** Mayo 2026  
**Curso:** Sistemas Operativos — ST0257  
**Implementación:** Python 3 / Linux

---

## 1. Descripción General

El sistema simula un ejecutor de procesos de lotes inspirado en los sistemas operativos de mainframe. Permite registrar programas y ficheros en un área de almacenamiento persistente (aralmac), y luego lanzar procesos de lotes que leen de su entrada estándar, procesan datos y retornan resultados por salida estándar.

### 1.1 Componentes

| Componente | Archivo | Rol |
|---|---|---|
| **ctrllt** | `src/ctrllt.py` | Pasarela central. Enruta peticiones del cliente a los servicios internos. |
| **gesfich** | `src/gesfich.py` | Gestor de ficheros. CRUD sobre archivos en aralmac. |
| **gesprog** | `src/gesprog.py` | Gestor de programas. CRUD sobre metadatos de ejecutables en aralmac. |
| **ejecutor** | `src/ejecutor.py` | Ejecutor de procesos de lotes. Lanza, monitorea y gestiona procesos hijo. |
| **aralmac** | directorio en disco | Área de almacenamiento persistente (directorio configurable con `-x`). |
| **cliente** | proporcionado por el profesor | Interfaz de usuario. Envía peticiones a ctrllt. |

### 1.2 Diagrama de Arquitectura

```
                          ┌──────────┐
          cliente 1 ─────▶│          │
          cliente 2 ─────▶│  ctrllt  │────▶ gesfich ──▶ aralmac/ficheros/
          cliente N ─────▶│          │────▶ gesprog ──▶ aralmac/programas/
                          │          │────▶ ejecutor ──▶ aralmac/ (lee ambos)
                          └──────────┘
```

Los clientes solo hablan con ctrllt. Los servicios internos nunca se comunican directamente entre sí.

---

## 2. Comunicación entre Procesos (IPC)

### 2.1 Mecanismo: Tuberías Nombradas (FIFOs)

La comunicación entre todos los componentes se realiza mediante **tuberías nombradas** (FIFOs, creadas con `os.mkfifo()` en Python / `mkfifo` en bash).

#### Por qué FIFOs y no sockets o pipes anónimas

Los FIFOs tienen nombre en el sistema de archivos, lo que permite que procesos independientes (lanzados en terminales separadas, sin relación padre-hijo) se conecten entre sí. Las pipes anónimas solo funcionan entre procesos relacionados por `fork()`. Los sockets serían más complejos para este caso.

#### Linux: Half-Duplex (dos FIFOs por conexión)

En Linux, los FIFOs son **unidireccionales**. Cada conexión entre dos procesos requiere dos FIFOs:

```
proceso A ──[pipe_req]──▶ proceso B   (A envía peticiones a B)
proceso A ◀──[pipe_res]── proceso B   (B envía respuestas a A)
```

#### Estrategia de apertura: O_RDWR para evitar interbloqueo

Si el servicio A abre su FIFO con `O_RDONLY`, se bloquea hasta que alguien la abra con `O_WRONLY`. Si ambos procesos esperan que el otro abra primero, hay interbloqueo (deadlock). La solución es abrir con `O_RDWR`:

```python
fd = os.open(ruta_fifo, os.O_RDWR)
```

Con `O_RDWR`, el kernel considera que la pipe tiene al menos un lector y un escritor (el mismo proceso), por lo que no bloquea. Esto permite que los servicios arranquen en cualquier orden sin coordinación.

**Tradeoff**: con `O_RDWR`, el proceso no recibe EOF cuando el escritor cierra la pipe. Para este sistema, el ciclo de vida se controla con la operación `"Terminar"`, no con el cierre físico de la pipe.

### 2.2 Convención de Nombres de Tuberías

| Conexión | Pipe de peticiones | Pipe de respuestas |
|---|---|---|
| cliente ↔ ctrllt | `/tmp/ejlotes_cli_req` | `/tmp/ejlotes_cli_res` |
| ctrllt ↔ gesfich | `/tmp/ejlotes_fich_req` | `/tmp/ejlotes_fich_res` |
| ctrllt ↔ gesprog | `/tmp/ejlotes_prog_req` | `/tmp/ejlotes_prog_res` |
| ctrllt ↔ ejecutor | `/tmp/ejlotes_ejec_req` | `/tmp/ejlotes_ejec_res` |

Estos nombres son configurables a través de los argumentos de línea de comandos.

### 2.3 Lectura de Mensajes: Byte a Byte

Los mensajes se leen byte a byte hasta encontrar el delimitador `\n`:

```python
buffer = b""
while True:
    byte = os.read(fd, 1)
    if byte == b"\n":
        break
    buffer += byte
return json.loads(buffer.decode("utf-8"))
```

**Por qué byte a byte y no bloques grandes**: Un FIFO puede devolver menos bytes de los pedidos si el buffer interno está parcialmente lleno. Si se leen bloques de 4096 bytes, se podría obtener parte de un mensaje y parte del siguiente, rompiendo el parse JSON. Leyendo byte a byte se garantiza exactamente un mensaje por llamada.

**Por qué esto es seguro (no hay mensajes partidos)**: El kernel de Linux garantiza que escrituras menores a `PIPE_BUF` (4096 bytes en Linux, igual a `MSG_MAX_LEN`) son **atómicas**: llegan completas o no llegan. Como cada mensaje JSON tiene máximo 4096 bytes, nunca llega "a la mitad".

---

## 3. Protocolo de Mensajes JSON

### 3.1 Formato de Petición (cliente → ctrllt → servicio)

```json
{"servicio":"<svc>","operacion":"<op>"[, campos adicionales...]}
```

### 3.2 Formato de Respuesta (servicio → ctrllt → cliente)

Éxito:
```json
{"estado":"ok"[, campos adicionales...]}
```

Error:
```json
{"estado":"error","mensaje":"<descripcion>"}
```

### 3.3 Identificadores

| Tipo | Formato | Ejemplo |
|---|---|---|
| Fichero | `f-XXXX` | `f-0001` |
| Programa | `p-XXXX` | `p-0001` |
| Ejecución | `e-XXXX` | `e-0001` |

Los contadores se almacenan en archivos JSON en el aralmac y persisten entre reinicios del servicio.

### 3.4 Tamaño Máximo

`MSG_MAX_LEN = 4096 bytes` por mensaje (definido en el enunciado).

---

## 4. Área de Almacenamiento (aralmac)

El aralmac es un directorio en disco configurado con el argumento `-x`. Su estructura:

```
aralmac/
├── ficheros/
│   ├── f-0001          ← contenido real del fichero (texto plano)
│   ├── f-0002
│   └── ...
├── ficheros_counter.json    ← {"next": 3}
├── programas/
│   ├── p-0001.json     ← metadatos del programa
│   ├── p-0002.json
│   └── ...
├── programas_counter.json   ← {"next": 3}
└── ejecuciones_counter.json ← {"next": 5}
```

Cada `p-XXXX.json` contiene:
```json
{
  "id-programa": "p-0001",
  "nombre":      "sort",
  "ejecutable":  "/usr/bin/sort",
  "args":        ["-r", "-n"],
  "env":         ["LANG=es_CO.UTF-8"]
}
```

---

## 5. Diseño por Componente

### 5.1 gesfich — Gestor de Ficheros

**Máquina de estados:**
```
inicio → Corriendo ──Suspender──▶ Suspendido
                   ◀──Reasumir──
         Corriendo  ──Terminar──▶ Terminado
         Suspendido ──Terminar──▶ Terminado
```

**En estado Suspendido**: solo acepta `Reasumir` y `Terminar`. Todas las operaciones CRUD devuelven `{"estado":"error","mensaje":"servicio suspendido"}`.

**Operaciones y respuestas del protocolo:** ver sección 3.9 del enunciado.

### 5.2 gesprog — Gestor de Programas

**Máquina de estados:** idéntica a gesfich.

**Diferencia clave vs gesfich**: en estado `Suspendido`, gesprog **sí permite** la operación `Leer`. Esto está explícito en el diagrama del enunciado (figura 4): la flecha "Leer" sale desde el estado `Suspendido`. Las demás operaciones CRUD (Guardar, Actualizar, Borrar) sí quedan bloqueadas.

**Motivo**: un administrador que suspende gesprog todavía puede necesitar consultar qué programas están registrados (para decidir cuál lanzar), aunque no quiera que se registren nuevos.

### 5.3 ejecutor — Ejecutor de Procesos

**Máquina de estados del servicio:**
```
inicio → Ejecutar ──Suspender──▶ Suspendidos ──Reasumir──▶ Ejecutar
         Ejecutar  ──Parar──────▶ Parar
         Suspendidos ──Parar───▶ Parar
         Parar ──(procesos=0)──▶ Terminado
```

**Ejecución de un proceso de lotes:**

1. Leer metadatos del programa desde `aralmac/programas/p-XXXX.json` (acceso directo al disco, sin pasar por gesprog)
2. Abrir ficheros de I/O en `aralmac/ficheros/f-XXXX` si se especificaron
3. Lanzar el proceso con `subprocess.Popen` (hace `fork+exec` internamente en Linux)
4. Registrar el proceso en el diccionario `procesos`
5. Lanzar un **hilo monitor** que llama a `popen.wait()` de forma bloqueante

**Por qué un hilo monitor por proceso:**
`popen.wait()` es bloqueante. Si se llamara en el hilo principal, el servicio quedaría congelado sin poder atender nuevas peticiones mientras el proceso de lotes corre. Con un hilo separado, el hilo principal queda libre. Se usa `threading.Thread` (no `multiprocessing`) porque:
- Los procesos son I/O-bound (esperan en pipes), no CPU-bound
- El hilo monitor necesita modificar el diccionario compartido `procesos`, lo que con `threading` no requiere serialización
- `threading` es más simple y con menos overhead para esta tarea

**Gestión de señales a procesos hijo:**
- Matar: `popen.kill()` → SIGKILL
- Suspender todos (Suspendidos): `os.kill(pid, signal.SIGSTOP)`
- Reasumir todos: `os.kill(pid, signal.SIGCONT)`

**Estado `Parar`**: el servicio deja de aceptar nuevas ejecuciones y espera a que todos los procesos activos terminen. La transición a `Terminado` la hace el **hilo monitor** del último proceso en terminar, no el hilo principal.

**Por qué `estado_servicio_ref` es una lista y no un string:**
Los strings en Python son inmutables. Si el hilo monitor necesita cambiar el estado a `Terminado`, necesita una referencia mutable. Una lista de un elemento `[estado]` permite que varios hilos lean y modifiquen el estado sin cambiar la referencia al objeto.

### 5.4 ctrllt — Controlador de Lotes

**Máquina de estados:** `Corriendo → Terminado`

**Lógica de enrutamiento:**
```
recibir(peticion)
├── peticion["servicio"] == "gesfich"  → escribir a pipe_fich_req, leer de pipe_fich_res
├── peticion["servicio"] == "gesprog"  → escribir a pipe_prog_req, leer de pipe_prog_res
├── peticion["servicio"] == "ejecutor" → escribir a pipe_ejec_req, leer de pipe_ejec_res
├── peticion["servicio"] == "ctrllt"   → manejar localmente (solo "Terminar")
└── otro servicio                      → retornar {"estado":"error","mensaje":"servicio desconocido"}
```

**Procesamiento secuencial**: ctrllt procesa una petición a la vez. Después de reenviar al servicio, espera la respuesta antes de aceptar la siguiente petición. Esto garantiza que cada respuesta corresponde exactamente a la petición enviada.

**Conflicto de argumentos `-c`**: el enunciado usa `-c` tanto para la pipe del cliente como para la pipe de respuesta de gesprog. En esta implementación se usa `--gres` para la pipe de respuesta de gesprog para evitar el conflicto en `argparse`.

**Operación Terminar del sistema:**
1. Envía `{"servicio":"gesfich","operacion":"Terminar"}` a gesfich y espera confirmación
2. Envía `{"servicio":"gesprog","operacion":"Terminar"}` a gesprog y espera confirmación
3. Envía `{"servicio":"ejecutor","operacion":"Parar"}` al ejecutor (que terminará solo cuando no queden procesos activos)
4. Envía `{"estado":"ok"}` al cliente
5. Sale del bucle principal

---

## 6. Instrucciones de Arranque

### Paso 1: Crear el aralmac

```bash
mkdir -p /tmp/aralmac/ficheros /tmp/aralmac/programas
```

### Paso 2: Lanzar los servicios (en terminales separadas o en background)

```bash
# Terminal 1
python3 src/gesfich.py -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res -x /tmp/aralmac

# Terminal 2
python3 src/gesprog.py -p /tmp/ejlotes_prog_req -c /tmp/ejlotes_prog_res -x /tmp/aralmac

# Terminal 3
python3 src/ejecutor.py -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res -x /tmp/aralmac

# Terminal 4
python3 src/ctrllt.py \
  -c /tmp/ejlotes_cli_req  -a /tmp/ejlotes_cli_res \
  -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res \
  -p /tmp/ejlotes_prog_req --gres /tmp/ejlotes_prog_res \
  -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res
```

### Paso 3: Conectar el cliente (cuando lo provea el profesor)

```bash
cliente -c /tmp/ejlotes_cli_req -a /tmp/ejlotes_cli_res
```

---

## 7. Ejemplo de Flujo Completo

```
1. Cliente → ctrllt: {"servicio":"gesfich","operacion":"Crear"}
   ctrllt  → gesfich: (mismo mensaje)
   gesfich → ctrllt:  {"estado":"ok","id-fichero":"f-0001"}
   ctrllt  → cliente: (misma respuesta)

2. Cliente → ctrllt: {"servicio":"gesfich","operacion":"Actualizar","id-fichero":"f-0001","ruta":"/home/user/datos.txt"}
   ... gesfich copia el contenido ...
   Respuesta: {"estado":"ok"}

3. Cliente → ctrllt: {"servicio":"gesprog","operacion":"Guardar","ejecutable":"/usr/bin/sort","args":["-r"]}
   ... gesprog guarda p-0001.json ...
   Respuesta: {"estado":"ok","id-programa":"p-0001"}

4. Cliente → ctrllt: {"servicio":"ejecutor","operacion":"Ejecutar","id-programa":"p-0001","stdin":"f-0001","stdout":"f-0002"}
   ... ejecutor lanza sort con f-0001 como stdin, escribe resultado en f-0002 ...
   Respuesta: {"estado":"ok","id-ejecucion":"e-0001"}

5. Cliente → ctrllt: {"servicio":"ejecutor","operacion":"Estado","id-ejecucion":"e-0001"}
   Respuesta: {"estado":"ok","id-ejecucion":"e-0001","id-programa":"p-0001","proceso-estado":"Terminado","codigo-salida":0}

6. Cliente → ctrllt: {"servicio":"gesfich","operacion":"Leer","id-fichero":"f-0002"}
   Respuesta: {"estado":"ok","contenido":"<datos ordenados>"}
```

---

## 8. Decisiones de Diseño

| Decisión | Alternativa descartada | Razón de la elección |
|---|---|---|
| `threading` para el monitor de procesos | `multiprocessing` | Los procesos son I/O-bound; el estado compartido (diccionario `procesos`) es más simple con threads que con colas de mensajes entre procesos |
| Apertura con `O_RDWR` | Apertura con `O_RDONLY`/`O_WRONLY` | Evita interbloqueo en el arranque; los servicios pueden iniciarse en cualquier orden |
| Lectura byte a byte | `readline()` en modo texto | Control explícito sobre el buffer; `readline()` sobre un fd POSIX puede tener comportamientos inesperados; con byte a byte el flujo es claro |
| Archivos JSON planos en aralmac | Base de datos SQLite | Más simple, legible, no requiere librerías externas; el enunciado dice que `<info-aralmac>` puede ser "la ruta de un directorio" |
| Procesamiento secuencial en ctrllt | Hilo por petición | Evita mezcla de respuestas en las pipes de los servicios; los servicios son secuenciales de todas formas |
| `subprocess.Popen` para lanzar procesos | `os.fork()` + `os.execve()` | `Popen` maneja los detalles de `fork+exec` correctamente (cierre de fds, señales, etc.); `os.fork()` en Python puede tener problemas con hilos activos |
| `daemon=True` en los hilos monitor | Hilos no-daemon | Permite que Python salga limpiamente sin esperar a que los hilos monitor terminen cuando el servicio principal recibe SIGTERM |

---

## 9. Estructura del Repositorio

```
/
├── docs/
│   └── diseño.md          ← este documento
├── src/
│   ├── ctrllt.py          ← pasarela central
│   ├── gesfich.py         ← gestor de ficheros
│   ├── gesprog.py         ← gestor de programas
│   └── ejecutor.py        ← ejecutor de procesos de lotes
├── tests/
│   └── prueba_manual.sh   ← script de prueba con echo + pipes
├── README.md
└── .gitignore
```
