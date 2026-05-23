# Ejecutor de Lotes — ST0257 Sistemas Operativos

Sistema de ejecución de procesos de lotes inspirado en sistemas operativos de mainframe.  
Implementado en **Python 3** para **Linux**, usando exclusivamente la biblioteca estándar.

## Componentes

| Servicio | Archivo | Descripción |
|---|---|---|
| `ctrllt` | `src/ctrllt.py` | Pasarela central: enruta peticiones del cliente a los servicios internos |
| `gesfich` | `src/gesfich.py` | Gestor de ficheros: CRUD sobre archivos en el aralmac |
| `gesprog` | `src/gesprog.py` | Gestor de programas: CRUD sobre metadatos de ejecutables |
| `ejecutor` | `src/ejecutor.py` | Ejecutor de procesos de lotes: lanza y gestiona procesos hijo |

## Requisitos

- Python 3.6 o superior
- Linux (se usan FIFOs POSIX, `os.mkfifo`, `signal.SIGSTOP`, etc.)
- Sin dependencias externas (solo biblioteca estándar)

## Arranque Rápido

### 1. Crear el área de almacenamiento

```bash
mkdir -p /tmp/aralmac/ficheros /tmp/aralmac/programas
```

### 2. Lanzar los servicios (cada uno en su terminal)

```bash
# Terminal 1 — gesfich
python3 src/gesfich.py -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res -x /tmp/aralmac

# Terminal 2 — gesprog
python3 src/gesprog.py -p /tmp/ejlotes_prog_req -c /tmp/ejlotes_prog_res -x /tmp/aralmac

# Terminal 3 — ejecutor
python3 src/ejecutor.py -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res -x /tmp/aralmac

# Terminal 4 — ctrllt (lanzar después de los servicios)
python3 src/ctrllt.py \
  -c /tmp/ejlotes_cli_req  -a /tmp/ejlotes_cli_res \
  -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res \
  -p /tmp/ejlotes_prog_req --gres /tmp/ejlotes_prog_res \
  -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res
```

### 3. Probar manualmente con echo

```bash
# Crear un fichero
echo '{"servicio":"gesfich","operacion":"Crear"}' > /tmp/ejlotes_cli_req &
head -n 1 /tmp/ejlotes_cli_res
# Respuesta: {"estado": "ok", "id-fichero": "f-0001"}

# Registrar un programa
echo '{"servicio":"gesprog","operacion":"Guardar","ejecutable":"/usr/bin/sort","args":["-r"]}' > /tmp/ejlotes_cli_req &
head -n 1 /tmp/ejlotes_cli_res
# Respuesta: {"estado": "ok", "id-programa": "p-0001"}
```

### 4. Ejecutar el script de pruebas completo

```bash
bash tests/prueba_manual.sh
```

## Protocolo de Mensajes

Mensajes JSON terminados en `\n`. Tamaño máximo: 4096 bytes.

**Petición:**
```json
{"servicio":"gesfich","operacion":"Crear"}
{"servicio":"gesprog","operacion":"Guardar","ejecutable":"/usr/bin/sort","args":["-r"]}
{"servicio":"ejecutor","operacion":"Ejecutar","id-programa":"p-0001","stdin":"f-0001","stdout":"f-0002"}
{"servicio":"ctrllt","operacion":"Terminar"}
```

**Respuesta exitosa:**
```json
{"estado":"ok","id-fichero":"f-0001"}
{"estado":"ok","id-programa":"p-0001"}
{"estado":"ok","id-ejecucion":"e-0001"}
```

**Respuesta de error:**
```json
{"estado":"error","mensaje":"fichero no encontrado"}
```

## Nota sobre el argumento `--gres`

El enunciado especifica `-c` para la pipe de respuesta de gesprog, pero `-c` ya se usa para la pipe del cliente en ctrllt. Para evitar el conflicto, esta implementación usa `--gres` para la pipe de respuesta de gesprog.

## Limpieza

```bash
rm -rf /tmp/aralmac /tmp/ejlotes_*
```

## Documentación

Ver `docs/diseño.md` para la arquitectura completa, el protocolo de mensajes y las decisiones de diseño.
