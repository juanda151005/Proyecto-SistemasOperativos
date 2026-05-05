# Diseño del Sistema: Ejecutor de Lotes

**Autor:** Juan David Velásquez Restrepo  
**Fecha:** Mayo 2026  
**Curso:** Sistemas Operativos  

---

## 1. Descripción General

El sistema simula un ejecutor de lotes inspirado en los sistemas operativos de mainframe. Permite registrar programas y ficheros, y ejecutar procesos de lotes que leen de entrada estándar, procesan datos y retornan resultados por salida estándar.

### 1.1 Componentes

| Componente | Rol |
|---|---|
| **cliente** | Interfaz de usuario. Envía peticiones CRUD de programas/ficheros y gestión de procesos de lotes. |
| **ctrllt** | Pasarela central. Recibe peticiones del cliente y las enruta al servicio correspondiente. |
| **gesfich** | Gestión de ficheros. CRUD sobre ficheros almacenados en `aralmac`. |
| **gesprog** | Gestión de programas. CRUD sobre programas almacenados en `aralmac`. |
| **ejecutor** | Ejecución de procesos de lotes. Crea, supervisa y gestiona procesos hijo. |
| **aralmac** | Área de almacenamiento persistente (directorio en disco). |

### 1.2 Diagrama de Arquitectura

```
                     ┌──────────┐
                     │ cliente 1 │──┐
                     └──────────┘  │
                     ┌──────────┐  │    ┌────────┐    ┌─────────┐
                     │ cliente 2 │──┼───▶│ ctrllt │───▶│ gesprog │──┐
                     └──────────┘  │    │        │    └─────────┘  │
                     ┌──────────┐  │    │        │    ┌─────────┐  │  ┌─────────┐
                     │ cliente N │──┘    │        │───▶│ gesfich │──┼─▶│ aralmac │
                     └──────────┘       │        │    └─────────┘  │  └─────────┘
                                        │        │    ┌──────────┐ │
                                        │        │───▶│ ejecutor │─┘
                                        └────────┘    └──────────┘
```

---

## 2. Comunicación entre Procesos

### 2.1 Mecanismo: Tuberías Nombradas (Named Pipes / FIFOs)

La comunicación entre todos los componentes se realiza mediante tuberías nombradas.

#### Linux (Half-Duplex)

En Linux, las tuberías nombradas (FIFOs) son **half-duplex** (unidireccionales). Por lo tanto, cada conexión entre dos procesos requiere **dos tuberías**:

- Una tubería para enviar peticiones (request).
- Una tubería para recibir respuestas (response).

**Creación:**
```bash
mkfifo /tmp/ejlotes_ctrllt_req    # cliente → ctrllt (peticiones)
mkfifo /tmp/ejlotes_ctrllt_res    # ctrllt → cliente (respuestas)
```

**API en C (Linux):**
```c
#include <sys/stat.h>
#include <fcntl.h>

// Crear FIFO
mkfifo("/tmp/ejlotes_ctrllt_req", 0666);

// Abrir para escritura (emisor)
int fd_write = open("/tmp/ejlotes_ctrllt_req", O_WRONLY);

// Abrir para lectura (receptor)
int fd_read = open("/tmp/ejlotes_ctrllt_req", O_RDONLY);
```

#### Windows 11 (Full-Duplex)

En Windows, las tuberías nombradas son **full-duplex** (bidireccionales). Solo se necesita **una tubería** por conexión.

**API en C (Windows):**
```c
#include <windows.h>

// Servidor: crear tubería
HANDLE hPipe = CreateNamedPipe(
    "\\\\.\\pipe\\ejlotes_ctrllt",
    PIPE_ACCESS_DUPLEX,
    PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
    PIPE_UNLIMITED_INSTANCES,
    4096, 4096, 0, NULL
);

// Cliente: conectar a tubería
HANDLE hPipe = CreateFile(
    "\\\\.\\pipe\\ejlotes_ctrllt",
    GENERIC_READ | GENERIC_WRITE,
    0, NULL, OPEN_EXISTING, 0, NULL
);
```

### 2.2 Convención de Nombres de Tuberías

| Conexión | Linux (req / res) | Windows |
|---|---|---|
| cliente ↔ ctrllt | `/tmp/ejlotes_cli_req`, `/tmp/ejlotes_cli_res` | `\\.\pipe\ejlotes_cli` |
| ctrllt ↔ gesfich | `/tmp/ejlotes_fich_req`, `/tmp/ejlotes_fich_res` | `\\.\pipe\ejlotes_fich` |
| ctrllt ↔ gesprog | `/tmp/ejlotes_prog_req`, `/tmp/ejlotes_prog_res` | `\\.\pipe\ejlotes_prog` |
| ctrllt ↔ ejecutor | `/tmp/ejlotes_ejec_req`, `/tmp/ejlotes_ejec_res` | `\\.\pipe\ejlotes_ejec` |

> **Nota:** Cuando hay múltiples clientes, cada cliente genera un par de tuberías únicas usando un sufijo (PID o UUID), por ejemplo: `/tmp/ejlotes_cli_12345_req`.

### 2.3 Protocolo de Mensajes

Cada mensaje se transmite como una cadena JSON terminada en un delimitador `\n` (newline). Esto permite al receptor leer línea por línea y deserializar cada mensaje.

**Flujo de comunicación:**

```
Cliente ──[JSON request]──▶ ctrllt ──[JSON request]──▶ servicio
Cliente ◀──[JSON response]── ctrllt ◀──[JSON response]── servicio
```

---

## 3. Formato de Mensajes JSON

### 3.1 Estructura General de Petición (Request)

```json
{
  "id": "req-001",
  "servicio": "gesfich | gesprog | ejecutor",
  "operacion": "<nombre-operacion>",
  "parametros": { }
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | string | Identificador único de la petición (para correlacionar con la respuesta). |
| `servicio` | string | Servicio destino: `"gesfich"`, `"gesprog"` o `"ejecutor"`. |
| `operacion` | string | Nombre de la operación a ejecutar. |
| `parametros` | object | Parámetros específicos de la operación. |

### 3.2 Estructura General de Respuesta (Response)

```json
{
  "id": "req-001",
  "estado": "ok | error",
  "datos": { },
  "mensaje": ""
}
```

| Campo | Tipo | Descripción |
|---|---|---|
| `id` | string | Mismo identificador de la petición original. |
| `estado` | string | `"ok"` si fue exitosa, `"error"` si falló. |
| `datos` | object | Datos de respuesta (varía según operación). |
| `mensaje` | string | Mensaje descriptivo (especialmente útil en errores). |

---

## 4. Diseño por Componente

### 4.1 ctrllt — Control de Lotes

#### Responsabilidad
Actúa como pasarela (gateway): recibe peticiones de los clientes, inspecciona el campo `servicio` del mensaje JSON y lo reenvía a la tubería del servicio correspondiente. Espera la respuesta del servicio y la reenvía al cliente.

#### Máquina de Estados

```
  ┌────────┐         ┌───────────┐         ┌───────────┐
  │ Inicio │────────▶│ Corriendo │────────▶│ Terminado │
  └────────┘         └───────────┘         └───────────┘
                      (Terminar)
```

- **Inicio → Corriendo:** Al arrancar, abre las tuberías y queda a la escucha.
- **Corriendo → Terminado:** Recibe señal de terminación, cierra tuberías y finaliza.

#### Lógica de Enrutamiento

```
recibir(peticion)
├── peticion.servicio == "gesfich"  → enviar a tubería de gesfich
├── peticion.servicio == "gesprog"  → enviar a tubería de gesprog
├── peticion.servicio == "ejecutor" → enviar a tubería de ejecutor
└── otro                            → responder error "servicio desconocido"
```

#### Concurrencia

Para soportar múltiples clientes, `ctrllt` debe:

- **Linux:** Usar `select()`, `poll()` o `epoll()` para multiplexar lecturas de múltiples tuberías, o crear un hilo (pthread) por cliente.
- **Windows:** Usar `WaitForMultipleObjects()` o hilos (`CreateThread`) con instancias separadas de la tubería nombrada.

---

### 4.2 gesfich — Gestión de Ficheros

#### Responsabilidad
CRUD de ficheros en el área de almacenamiento `aralmac`.

#### Máquina de Estados

```
              Crear/Leer/Actualizar/Borrar
                        ┌──────┐
                        ▼      │
  ┌────────┐    ┌───────────┐  │     ┌───────────┐
  │ Inicio │───▶│ Corriendo │──┘  ┌─▶│ Terminado │
  └────────┘    └───────────┘     │  └───────────┘
                   │    ▲         │
          Suspender│    │Reasumir │Terminar
                   ▼    │         │
                ┌────────────┐    │
                │ Suspendido │────┘
                └────────────┘
```

- **Suspendido:** No procesa peticiones CRUD. Solo acepta `Reasumir` y `Terminar`.

#### Operaciones — Mensajes JSON

**Crear fichero:**

Petición:
```json
{
  "id": "req-101",
  "servicio": "gesfich",
  "operacion": "crear",
  "parametros": {}
}
```

Respuesta exitosa:
```json
{
  "id": "req-101",
  "estado": "ok",
  "datos": { "id_fichero": "f-0001" },
  "mensaje": "Fichero creado exitosamente."
}
```

**Leer fichero (por ID):**

Petición:
```json
{
  "id": "req-102",
  "servicio": "gesfich",
  "operacion": "leer",
  "parametros": { "id_fichero": "f-0001" }
}
```

Respuesta exitosa:
```json
{
  "id": "req-102",
  "estado": "ok",
  "datos": {
    "id_fichero": "f-0001",
    "contenido": "contenido del fichero en base64 o texto plano"
  },
  "mensaje": ""
}
```

**Leer todos los ficheros:**

Petición:
```json
{
  "id": "req-103",
  "servicio": "gesfich",
  "operacion": "leer",
  "parametros": {}
}
```

Respuesta exitosa:
```json
{
  "id": "req-103",
  "estado": "ok",
  "datos": {
    "ficheros": [
      { "id_fichero": "f-0001", "tamaño": 1024 },
      { "id_fichero": "f-0002", "tamaño": 512 }
    ]
  },
  "mensaje": ""
}
```

**Actualizar fichero:**

Petición:
```json
{
  "id": "req-104",
  "servicio": "gesfich",
  "operacion": "actualizar",
  "parametros": {
    "id_fichero": "f-0001",
    "ruta_fichero": "/ruta/al/fichero/origen.txt"
  }
}
```

Respuesta exitosa:
```json
{
  "id": "req-104",
  "estado": "ok",
  "datos": { "id_fichero": "f-0001" },
  "mensaje": "Fichero actualizado exitosamente."
}
```

**Borrar fichero:**

Petición:
```json
{
  "id": "req-105",
  "servicio": "gesfich",
  "operacion": "borrar",
  "parametros": { "id_fichero": "f-0001" }
}
```

Respuesta exitosa:
```json
{
  "id": "req-105",
  "estado": "ok",
  "datos": {},
  "mensaje": "Fichero borrado exitosamente."
}
```

**Suspender / Reasumir / Terminar:**

Petición (ejemplo Suspender):
```json
{
  "id": "req-106",
  "servicio": "gesfich",
  "operacion": "suspender",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-106",
  "estado": "ok",
  "datos": { "estado_servicio": "suspendido" },
  "mensaje": "Servicio suspendido."
}
```

---

### 4.3 gesprog — Gestión de Programas

#### Responsabilidad
CRUD de programas (ejecutables) en el área de almacenamiento `aralmac`.

#### Máquina de Estados

```
              Guardar/Leer/Actualizar/Borrar
                        ┌──────┐
                        ▼      │
  ┌────────┐    ┌───────────┐  │     ┌───────────┐
  │ Inicio │───▶│ Corriendo │──┘  ┌─▶│ Terminado │
  └────────┘    └───────────┘     │  └───────────┘
                   │    ▲         │
          Suspender│    │Reasumir │Terminar
                   ▼    │         │
                ┌────────────┐    │
                │ Suspendido │────┘
                └────────────┘
```

> **Nota:** En estado `Suspendido`, gesprog solo acepta operación `Leer` (además de `Reasumir` y `Terminar`), según la especificación.

#### Operaciones — Mensajes JSON

**Guardar programa:**

Petición:
```json
{
  "id": "req-201",
  "servicio": "gesprog",
  "operacion": "guardar",
  "parametros": {
    "ejecutable": "/usr/bin/sort",
    "argumentos": ["-r", "-n"],
    "ambiente": {
      "LANG": "es_CO.UTF-8",
      "PATH": "/usr/bin:/bin"
    }
  }
}
```

Respuesta exitosa:
```json
{
  "id": "req-201",
  "estado": "ok",
  "datos": { "id_programa": "p-0001" },
  "mensaje": "Programa registrado exitosamente."
}
```

**Leer programa (por ID):**

Petición:
```json
{
  "id": "req-202",
  "servicio": "gesprog",
  "operacion": "leer",
  "parametros": { "id_programa": "p-0001" }
}
```

Respuesta:
```json
{
  "id": "req-202",
  "estado": "ok",
  "datos": {
    "id_programa": "p-0001",
    "ejecutable": "/usr/bin/sort",
    "argumentos": ["-r", "-n"],
    "ambiente": { "LANG": "es_CO.UTF-8", "PATH": "/usr/bin:/bin" }
  },
  "mensaje": ""
}
```

**Leer todos los programas:**

Petición:
```json
{
  "id": "req-203",
  "servicio": "gesprog",
  "operacion": "leer",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-203",
  "estado": "ok",
  "datos": {
    "programas": [
      { "id_programa": "p-0001", "ejecutable": "/usr/bin/sort" },
      { "id_programa": "p-0002", "ejecutable": "/usr/local/bin/miprog" }
    ]
  },
  "mensaje": ""
}
```

**Actualizar programa:**

Petición:
```json
{
  "id": "req-204",
  "servicio": "gesprog",
  "operacion": "actualizar",
  "parametros": {
    "id_programa": "p-0001",
    "ejecutable": "/usr/bin/sort",
    "argumentos": ["-r", "-n", "-k2"],
    "ambiente": { "LANG": "en_US.UTF-8" }
  }
}
```

**Borrar programa:**

Petición:
```json
{
  "id": "req-205",
  "servicio": "gesprog",
  "operacion": "borrar",
  "parametros": { "id_programa": "p-0001" }
}
```

---

### 4.4 ejecutor — Ejecución de Procesos de Lotes

#### Responsabilidad
Ejecutar procesos de lotes combinando programas registrados en `gesprog` con ficheros registrados en `gesfich`. Gestionar su ciclo de vida.

#### Máquina de Estados

```
  ┌────────┐    ┌──────────┐  Ejecutar/Estado/Matar
  │ Inicio │───▶│ Ejecutar │◀──────────────────────┐
  └────────┘    └──────────┘───────────────────────┐│
                   │    ▲                          ││
          Suspender│    │Reasumir                  ││
                   ▼    │                          ││
                ┌─────────────┐   Parar     ┌──────┴┤
                │ Suspendidos │───────────▶ │ Parar ││
                └─────────────┘             └───────┘│
                                  Procesos=0         │
                                ┌───────────┐        │
                                │ Terminar  │◀───────┘
                                └───────────┘
```

#### Definición de un Proceso de Lotes

Un proceso de lotes combina un programa con ficheros de entrada/salida:

```json
{
  "id_programa": "p-0001",
  "id_fichero_entrada": "f-0001",
  "id_fichero_salida": "f-0002"
}
```

El ejecutor:
1. Lee el programa registrado con `id_programa` de `aralmac`.
2. Lee el contenido del fichero de entrada (`id_fichero_entrada`) de `aralmac`.
3. Crea un proceso hijo ejecutando el programa, pasando el contenido del fichero como entrada estándar (stdin).
4. Captura la salida estándar (stdout) del proceso hijo.
5. Escribe el resultado en el fichero de salida (`id_fichero_salida`) en `aralmac`.

#### Operaciones — Mensajes JSON

**Ejecutar proceso de lotes:**

Petición:
```json
{
  "id": "req-301",
  "servicio": "ejecutor",
  "operacion": "ejecutar",
  "parametros": {
    "lote": {
      "id_programa": "p-0001",
      "id_fichero_entrada": "f-0001",
      "id_fichero_salida": "f-0002"
    }
  }
}
```

Respuesta exitosa:
```json
{
  "id": "req-301",
  "estado": "ok",
  "datos": { "id_lote": "l-0001" },
  "mensaje": "Proceso de lotes iniciado."
}
```

**Consultar estado de un lote:**

Petición:
```json
{
  "id": "req-302",
  "servicio": "ejecutor",
  "operacion": "estado",
  "parametros": { "id_lote": "l-0001" }
}
```

Respuesta:
```json
{
  "id": "req-302",
  "estado": "ok",
  "datos": {
    "id_lote": "l-0001",
    "estado_lote": "ejecutando | terminado | error",
    "pid": 12345,
    "codigo_salida": null
  },
  "mensaje": ""
}
```

**Listar todos los procesos de lotes:**

Petición:
```json
{
  "id": "req-303",
  "servicio": "ejecutor",
  "operacion": "estado",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-303",
  "estado": "ok",
  "datos": {
    "lotes": [
      { "id_lote": "l-0001", "estado_lote": "ejecutando", "id_programa": "p-0001" },
      { "id_lote": "l-0002", "estado_lote": "terminado", "id_programa": "p-0003" }
    ]
  },
  "mensaje": ""
}
```

**Matar un proceso de lotes:**

Petición:
```json
{
  "id": "req-304",
  "servicio": "ejecutor",
  "operacion": "matar",
  "parametros": { "id_lote": "l-0001" }
}
```

Respuesta:
```json
{
  "id": "req-304",
  "estado": "ok",
  "datos": { "id_lote": "l-0001" },
  "mensaje": "Proceso de lotes terminado forzosamente."
}
```

**Suspender el ejecutor:**

Petición:
```json
{
  "id": "req-305",
  "servicio": "ejecutor",
  "operacion": "suspender",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-305",
  "estado": "ok",
  "datos": { "estado_servicio": "suspendido" },
  "mensaje": "Ejecutor suspendido."
}
```

**Reasumir el ejecutor:**

Petición:
```json
{
  "id": "req-306",
  "servicio": "ejecutor",
  "operacion": "reasumir",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-306",
  "estado": "ok",
  "datos": { "estado_servicio": "ejecutando" },
  "mensaje": "Ejecutor reasumido."
}
```

**Parar el ejecutor:**

Petición:
```json
{
  "id": "req-307",
  "servicio": "ejecutor",
  "operacion": "parar",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-307",
  "estado": "ok",
  "datos": { "estado_servicio": "parando" },
  "mensaje": "Ejecutor en estado Parar. Terminará cuando no queden procesos activos."
}
```

**Terminar el ejecutor:**

Petición:
```json
{
  "id": "req-308",
  "servicio": "ejecutor",
  "operacion": "terminar",
  "parametros": {}
}
```

Respuesta:
```json
{
  "id": "req-308",
  "estado": "ok",
  "datos": { "estado_servicio": "terminado" },
  "mensaje": "Ejecutor terminado."
}
```

---

## 5. Área de Almacenamiento (aralmac)

### 5.1 Estructura de Directorios

`aralmac` es un directorio en el sistema de archivos que funciona como almacenamiento persistente para ficheros y programas.

```
aralmac/
├── ficheros/
│   ├── f-0001          # contenido del fichero
│   ├── f-0002
│   └── ...
├── programas/
│   ├── p-0001.json     # metadatos del programa
│   ├── p-0002.json
│   └── ...
└── metadata/
    ├── ficheros.json   # índice de ficheros (contadores, lista)
    └── programas.json  # índice de programas (contadores, lista)
```

### 5.2 Formato de Metadatos de Programa

Cada programa registrado se almacena como un archivo JSON:

```json
{
  "id_programa": "p-0001",
  "ejecutable": "/usr/bin/sort",
  "argumentos": ["-r", "-n"],
  "ambiente": {
    "LANG": "es_CO.UTF-8",
    "PATH": "/usr/bin:/bin"
  },
  "fecha_registro": "2026-05-03T10:30:00Z"
}
```

### 5.3 Gestión de Identificadores

Los identificadores siguen un esquema secuencial:

- **Ficheros:** `f-XXXX` → `f-0001`, `f-0002`, ..., `f-9999`
- **Programas:** `p-XXXX` → `p-0001`, `p-0002`, ..., `p-9999`
- **Lotes:** `l-XXXX` → `l-0001`, `l-0002`, ..., `l-9999`

El contador se almacena en los archivos de metadata y se incrementa en cada creación.

---

## 6. Manejo de Errores

### 6.1 Códigos de Error

| Código | Descripción |
|---|---|
| `ERR_SERVICIO_DESCONOCIDO` | El campo `servicio` no corresponde a ningún servicio válido. |
| `ERR_OPERACION_DESCONOCIDA` | La operación solicitada no existe para el servicio. |
| `ERR_FICHERO_NO_ENCONTRADO` | El `id_fichero` no existe en aralmac. |
| `ERR_PROGRAMA_NO_ENCONTRADO` | El `id_programa` no existe en aralmac. |
| `ERR_LOTE_NO_ENCONTRADO` | El `id_lote` no existe. |
| `ERR_EJECUTABLE_INVALIDO` | El ejecutable no existe o no tiene permisos. |
| `ERR_RUTA_INVALIDA` | La ruta del fichero origen no es válida o no existe. |
| `ERR_SERVICIO_SUSPENDIDO` | El servicio está suspendido y no acepta esa operación. |
| `ERR_TRANSICION_INVALIDA` | La transición de estado solicitada no es válida. |
| `ERR_PARAMETROS_INVALIDOS` | Faltan parámetros o tienen formato incorrecto. |
| `ERR_EJECUCION_FALLIDA` | El proceso hijo no pudo iniciarse. |

### 6.2 Formato de Respuesta de Error

```json
{
  "id": "req-999",
  "estado": "error",
  "datos": {
    "codigo": "ERR_FICHERO_NO_ENCONTRADO"
  },
  "mensaje": "El fichero con id 'f-9999' no existe en el almacenamiento."
}
```

---

## 7. Gestión de Procesos por Sistema Operativo

### 7.1 Linux

| Acción | Llamada al sistema |
|---|---|
| Crear proceso | `fork()` + `execve()` |
| Redirigir stdin | `dup2(fd_entrada, STDIN_FILENO)` |
| Capturar stdout | `dup2(fd_salida, STDOUT_FILENO)` con `pipe()` |
| Esperar proceso | `waitpid(pid, &status, WNOHANG)` |
| Matar proceso | `kill(pid, SIGKILL)` |
| Suspender proceso | `kill(pid, SIGSTOP)` |
| Reanudar proceso | `kill(pid, SIGCONT)` |

### 7.2 Windows 11

| Acción | API Win32 |
|---|---|
| Crear proceso | `CreateProcess()` con `STARTUPINFO` |
| Redirigir stdin | `STARTUPINFO.hStdInput` con `CreatePipe()` |
| Capturar stdout | `STARTUPINFO.hStdOutput` con `CreatePipe()` |
| Esperar proceso | `WaitForSingleObject(hProcess, 0)` |
| Matar proceso | `TerminateProcess(hProcess, 1)` |
| Suspender proceso | `SuspendThread(hThread)` |
| Reanudar proceso | `ResumeThread(hThread)` |

---

## 8. Invocación de los Componentes

### 8.1 Ejemplo de Arranque del Sistema (Linux)

```bash
# 1. Crear directorio de almacenamiento
mkdir -p /tmp/aralmac/ficheros /tmp/aralmac/programas /tmp/aralmac/metadata

# 2. Iniciar servicios (cada uno en una terminal o en background)
./gesfich -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res -x /tmp/aralmac &
./gesprog -p /tmp/ejlotes_prog_req -c /tmp/ejlotes_prog_res -x /tmp/aralmac &
./ejecutor -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res -x /tmp/aralmac &

# 3. Iniciar controlador de lotes
./ctrllt -c /tmp/ejlotes_cli_req -a /tmp/ejlotes_cli_res \
         -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res \
         -p /tmp/ejlotes_prog_req -c /tmp/ejlotes_prog_res \
         -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res &

# 4. Ejecutar cliente
./cliente -c /tmp/ejlotes_cli_req -a /tmp/ejlotes_cli_res
```

### 8.2 Ejemplo de Arranque del Sistema (Windows)

```powershell
# 1. Crear directorio de almacenamiento
mkdir C:\aralmac\ficheros
mkdir C:\aralmac\programas
mkdir C:\aralmac\metadata

# 2. Iniciar servicios
Start-Process .\gesfich.exe -ArgumentList "-f \\.\pipe\ejlotes_fich -x C:\aralmac"
Start-Process .\gesprog.exe -ArgumentList "-p \\.\pipe\ejlotes_prog -x C:\aralmac"
Start-Process .\ejecutor.exe -ArgumentList "-e \\.\pipe\ejlotes_ejec -x C:\aralmac"

# 3. Iniciar controlador
Start-Process .\ctrllt.exe -ArgumentList "-c \\.\pipe\ejlotes_cli -f \\.\pipe\ejlotes_fich -p \\.\pipe\ejlotes_prog -e \\.\pipe\ejlotes_ejec"

# 4. Ejecutar cliente
.\cliente.exe -c \\.\pipe\ejlotes_cli
```

---

## 9. Ejemplo de Flujo Completo

A continuación se ilustra un escenario completo de ejecución de un proceso de lotes:

### Paso 1: Crear un fichero de entrada

```
cliente → ctrllt → gesfich
```

```json
{ "id": "r1", "servicio": "gesfich", "operacion": "crear", "parametros": {} }
```
Respuesta: `{ "datos": { "id_fichero": "f-0001" } }`

### Paso 2: Cargar datos al fichero de entrada

```json
{
  "id": "r2", "servicio": "gesfich", "operacion": "actualizar",
  "parametros": { "id_fichero": "f-0001", "ruta_fichero": "/home/user/datos.txt" }
}
```

### Paso 3: Crear un fichero de salida

```json
{ "id": "r3", "servicio": "gesfich", "operacion": "crear", "parametros": {} }
```
Respuesta: `{ "datos": { "id_fichero": "f-0002" } }`

### Paso 4: Registrar el programa

```json
{
  "id": "r4", "servicio": "gesprog", "operacion": "guardar",
  "parametros": {
    "ejecutable": "/usr/bin/sort",
    "argumentos": ["-r", "-n"],
    "ambiente": { "LANG": "es_CO.UTF-8" }
  }
}
```
Respuesta: `{ "datos": { "id_programa": "p-0001" } }`

### Paso 5: Ejecutar el proceso de lotes

```json
{
  "id": "r5", "servicio": "ejecutor", "operacion": "ejecutar",
  "parametros": {
    "lote": {
      "id_programa": "p-0001",
      "id_fichero_entrada": "f-0001",
      "id_fichero_salida": "f-0002"
    }
  }
}
```
Respuesta: `{ "datos": { "id_lote": "l-0001" } }`

### Paso 6: Consultar estado

```json
{ "id": "r6", "servicio": "ejecutor", "operacion": "estado", "parametros": { "id_lote": "l-0001" } }
```

### Paso 7: Leer resultado

```json
{ "id": "r7", "servicio": "gesfich", "operacion": "leer", "parametros": { "id_fichero": "f-0002" } }
```

---

## 10. Tecnologías y Herramientas

| Aspecto | Decisión |
|---|---|
| Lenguaje | C (para acceso directo a llamadas del sistema) |
| Compilador Linux | GCC |
| Compilador Windows | MSVC o MinGW-w64 |
| Formato de mensajes | JSON |
| Librería JSON (C) | cJSON (ligera, dominio público) |
| Sistema de build | Makefile (Linux), CMake (multiplataforma) |
| Control de versiones | Git (GitHub/GitLab) |
| Documentación | Markdown |

---

## 11. Estructura del Repositorio

```
proyecto-ejecutor-lotes/
├── docs/
│   └── Diseño.md                 # Este documento
├── src/
│   ├── common/
│   │   ├── protocolo.h           # Estructuras y funciones de serialización JSON
│   │   ├── protocolo.c
│   │   ├── tuberias.h            # Abstracción de tuberías (Linux/Windows)
│   │   └── tuberias.c
│   ├── ctrllt/
│   │   ├── ctrllt.c
│   │   └── ctrllt.h
│   ├── gesfich/
│   │   ├── gesfich.c
│   │   └── gesfich.h
│   ├── gesprog/
│   │   ├── gesprog.c
│   │   └── gesprog.h
│   ├── ejecutor/
│   │   ├── ejecutor.c
│   │   └── ejecutor.h
│   └── cliente/
│       └── (proporcionado por el profesor)
├── lib/
│   └── cjson/                    # Librería cJSON
├── Makefile                      # Build Linux
├── CMakeLists.txt                # Build multiplataforma
└── README.md
```

---

## 12. Plan de Implementación

| Fase | Descripción | Prioridad |
|---|---|---|
| 1 | Módulo común: protocolo JSON + abstracción de tuberías | Alta |
| 2 | gesfich: CRUD de ficheros + máquina de estados | Alta |
| 3 | gesprog: CRUD de programas + máquina de estados | Alta |
| 4 | ejecutor: creación y gestión de procesos hijo | Alta |
| 5 | ctrllt: enrutamiento + concurrencia multicliente | Alta |
| 6 | Integración y pruebas end-to-end | Alta |
| 7 | Portabilidad Windows (si aplica) | Media |