#!/usr/bin/env bash
# =============================================================================
# prueba_manual.sh — Script de pruebas manuales del sistema de lotes
#
# Qué hace este script:
#   Prueba cada servicio enviando mensajes JSON por las tuberías nombradas
#   usando 'echo' y redirigiendo la salida con '>'. Permite verificar que
#   cada operación del protocolo funciona correctamente sin necesitar al
#   cliente oficial.
#
# Cómo usarlo:
#   1. En terminales separadas, lanzar los servicios (ver sección ARRANQUE)
#   2. En otra terminal, ejecutar este script: bash tests/prueba_manual.sh
#
# Estructura:
#   - Sección 1: prueba gesfich directamente (sin ctrllt)
#   - Sección 2: prueba gesprog directamente (sin ctrllt)
#   - Sección 3: prueba el flujo completo a través de ctrllt
#   - Sección 4: prueba el ejecutor con un proceso de lotes real
# =============================================================================

set -e  # Detener si cualquier comando falla

# ── Colores para salida legible ───────────────────────────────────────────────
VERDE="\033[0;32m"
ROJO="\033[0;31m"
CYAN="\033[0;36m"
RESET="\033[0m"

# ── Rutas de las tuberías y del aralmac ───────────────────────────────────────
# Ajusta estas rutas si lanzaste los servicios con nombres diferentes
ARALMAC="/tmp/aralmac_test"

PIPE_CLI_REQ="/tmp/ejlotes_cli_req"
PIPE_CLI_RES="/tmp/ejlotes_cli_res"
PIPE_FICH_REQ="/tmp/ejlotes_fich_req"
PIPE_FICH_RES="/tmp/ejlotes_fich_res"
PIPE_PROG_REQ="/tmp/ejlotes_prog_req"
PIPE_PROG_RES="/tmp/ejlotes_prog_res"
PIPE_EJEC_REQ="/tmp/ejlotes_ejec_req"
PIPE_EJEC_RES="/tmp/ejlotes_ejec_res"

# Tiempo de espera entre envío y lectura (segundos)
ESPERA=0.5

# =============================================================================
# FUNCIONES AUXILIARES
# =============================================================================

titulo() {
    echo ""
    echo -e "${CYAN}══════════════════════════════════════════════${RESET}"
    echo -e "${CYAN}  $1${RESET}"
    echo -e "${CYAN}══════════════════════════════════════════════${RESET}"
}

prueba() {
    # $1 = descripción de la prueba
    # $2 = mensaje JSON a enviar
    # $3 = pipe de peticiones
    # $4 = pipe de respuestas
    echo ""
    echo -e "${VERDE}▶ Prueba: $1${RESET}"
    echo -e "  Enviando: $2"

    # Enviar el mensaje JSON (echo agrega automáticamente \n al final)
    # El '&' corre el echo en background para no bloquear si la pipe está llena
    echo "$2" > "$3" &
    local pid_echo=$!

    # Esperar un poco para que el servicio procese la petición
    sleep "$ESPERA"

    # Leer la respuesta (timeout para no bloquear si el servicio no responde)
    local respuesta
    respuesta=$(timeout 2 head -n 1 "$4" 2>/dev/null || echo '{"estado":"error","mensaje":"sin respuesta (timeout)"}')

    echo -e "  Respuesta: $respuesta"

    # Esperar a que el echo termine
    wait "$pid_echo" 2>/dev/null || true
}

verificar_pipes() {
    # Verificar que las pipes existen antes de empezar
    for pipe in "$@"; do
        if [ ! -p "$pipe" ]; then
            echo -e "${ROJO}ERROR: La tubería $pipe no existe.${RESET}"
            echo "  Verifica que los servicios están corriendo."
            exit 1
        fi
    done
    echo -e "${VERDE}✓ Todas las tuberías están disponibles${RESET}"
}

# =============================================================================
# INSTRUCCIONES DE ARRANQUE
# =============================================================================

titulo "INSTRUCCIONES DE ARRANQUE DEL SISTEMA"

cat << 'EOF'
Antes de correr este script, abre 4 terminales y ejecuta:

Terminal 1 — gesfich:
  python3 src/gesfich.py -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res -x /tmp/aralmac_test

Terminal 2 — gesprog:
  python3 src/gesprog.py -p /tmp/ejlotes_prog_req -c /tmp/ejlotes_prog_res -x /tmp/aralmac_test

Terminal 3 — ejecutor:
  python3 src/ejecutor.py -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res -x /tmp/aralmac_test

Terminal 4 — ctrllt:
  python3 src/ctrllt.py \
    -c /tmp/ejlotes_cli_req -a /tmp/ejlotes_cli_res \
    -f /tmp/ejlotes_fich_req -b /tmp/ejlotes_fich_res \
    -p /tmp/ejlotes_prog_req --gres /tmp/ejlotes_prog_res \
    -e /tmp/ejlotes_ejec_req -d /tmp/ejlotes_ejec_res

Luego espera 2 segundos y ejecuta: bash tests/prueba_manual.sh
EOF

echo ""
read -p "Presiona ENTER cuando los servicios estén corriendo..."

# =============================================================================
# SECCIÓN 1: PRUEBAS DE gesfich (DIRECTO, sin ctrllt)
# =============================================================================

titulo "SECCIÓN 1: Pruebas de gesfich (conexión directa)"

# Verificar que las pipes de gesfich existen
verificar_pipes "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.1: Crear un fichero
# Qué se prueba: operación Crear sin parámetros; debe retornar f-0001
prueba "Crear fichero (debe retornar f-0001)" \
    '{"servicio":"gesfich","operacion":"Crear"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.2: Crear otro fichero
# Qué se prueba: el contador se incrementa; debe retornar f-0002
prueba "Crear otro fichero (debe retornar f-0002)" \
    '{"servicio":"gesfich","operacion":"Crear"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.3: Leer todos los ficheros
# Qué se prueba: Leer sin id-fichero; debe retornar ["f-0001","f-0002"]
prueba "Listar todos los ficheros (debe retornar f-0001 y f-0002)" \
    '{"servicio":"gesfich","operacion":"Leer"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# Crear un archivo de prueba para la operación Actualizar
echo "Hola desde el archivo de prueba" > /tmp/datos_prueba.txt

# PRUEBA 1.4: Actualizar f-0001 con el archivo de prueba
# Qué se prueba: copiar contenido externo al fichero en aralmac
prueba "Actualizar f-0001 con archivo /tmp/datos_prueba.txt" \
    '{"servicio":"gesfich","operacion":"Actualizar","id-fichero":"f-0001","ruta":"/tmp/datos_prueba.txt"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.5: Leer el contenido de f-0001 por ID
# Qué se prueba: Leer CON id-fichero; debe retornar el contenido que acabamos de cargar
prueba "Leer contenido de f-0001 (debe mostrar el texto cargado)" \
    '{"servicio":"gesfich","operacion":"Leer","id-fichero":"f-0001"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.6: Leer un fichero que no existe
# Qué se prueba: manejo de error con ID inválido
prueba "Leer fichero inexistente f-9999 (debe retornar error)" \
    '{"servicio":"gesfich","operacion":"Leer","id-fichero":"f-9999"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.7: Borrar f-0002
# Qué se prueba: eliminar un fichero existente
prueba "Borrar f-0002 (debe retornar ok)" \
    '{"servicio":"gesfich","operacion":"Borrar","id-fichero":"f-0002"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.8: Borrar nuevamente (debe dar error)
# Qué se prueba: el fichero ya no existe después de borrar
prueba "Borrar f-0002 de nuevo (debe retornar error: fichero no encontrado)" \
    '{"servicio":"gesfich","operacion":"Borrar","id-fichero":"f-0002"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.9: Suspender el servicio
# Qué se prueba: transición Corriendo → Suspendido
prueba "Suspender gesfich (debe retornar ok)" \
    '{"servicio":"gesfich","operacion":"Suspender"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.10: Intentar Crear mientras suspendido
# Qué se prueba: operación CRUD bloqueada en estado Suspendido
prueba "Crear mientras suspendido (debe retornar error: servicio suspendido)" \
    '{"servicio":"gesfich","operacion":"Crear"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.11: Reasumir el servicio
# Qué se prueba: transición Suspendido → Corriendo
prueba "Reasumir gesfich (debe retornar ok)" \
    '{"servicio":"gesfich","operacion":"Reasumir"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# PRUEBA 1.12: Crear después de reasumir (debe funcionar)
# Qué se prueba: el servicio volvió al estado Corriendo correctamente
prueba "Crear fichero tras Reasumir (debe funcionar normalmente)" \
    '{"servicio":"gesfich","operacion":"Crear"}' \
    "$PIPE_FICH_REQ" "$PIPE_FICH_RES"

# =============================================================================
# SECCIÓN 2: PRUEBAS DE gesprog (DIRECTO, sin ctrllt)
# =============================================================================

titulo "SECCIÓN 2: Pruebas de gesprog (conexión directa)"

verificar_pipes "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.1: Guardar un programa
# Qué se prueba: registrar /usr/bin/sort con argumentos y variables de entorno
prueba "Guardar programa /usr/bin/sort (debe retornar p-0001)" \
    '{"servicio":"gesprog","operacion":"Guardar","ejecutable":"/usr/bin/sort","args":["-r","-n"],"env":["LANG=es_CO.UTF-8"]}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.2: Guardar /bin/cat
prueba "Guardar programa /bin/cat (debe retornar p-0002)" \
    '{"servicio":"gesprog","operacion":"Guardar","ejecutable":"/bin/cat"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.3: Leer metadatos de p-0001
# Qué se prueba: Leer CON id-programa; debe retornar el objeto de metadatos
prueba "Leer metadatos de p-0001 (debe mostrar nombre, ejecutable, args, env)" \
    '{"servicio":"gesprog","operacion":"Leer","id-programa":"p-0001"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.4: Listar todos los programas
prueba "Listar todos los programas (debe retornar p-0001 y p-0002)" \
    '{"servicio":"gesprog","operacion":"Leer"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.5: Guardar ejecutable inválido (no existe)
# Qué se prueba: validación del ejecutable
prueba "Guardar ejecutable inexistente (debe retornar error)" \
    '{"servicio":"gesprog","operacion":"Guardar","ejecutable":"/ruta/que/no/existe"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.6: Suspender gesprog
prueba "Suspender gesprog" \
    '{"servicio":"gesprog","operacion":"Suspender"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.7: Leer mientras suspendido (DEBE FUNCIONAR en gesprog)
# Qué se prueba: a diferencia de gesfich, gesprog permite Leer en Suspendido
prueba "Leer p-0001 mientras suspendido (debe FUNCIONAR, es diferente a gesfich)" \
    '{"servicio":"gesprog","operacion":"Leer","id-programa":"p-0001"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.8: Reasumir gesprog
prueba "Reasumir gesprog" \
    '{"servicio":"gesprog","operacion":"Reasumir"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# PRUEBA 2.9: Borrar p-0002
prueba "Borrar p-0002 (debe retornar ok)" \
    '{"servicio":"gesprog","operacion":"Borrar","id-programa":"p-0002"}' \
    "$PIPE_PROG_REQ" "$PIPE_PROG_RES"

# =============================================================================
# SECCIÓN 3: PRUEBAS A TRAVÉS DE ctrllt
# =============================================================================

titulo "SECCIÓN 3: Pruebas del enrutamiento por ctrllt"

verificar_pipes "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# PRUEBA 3.1: Crear fichero a través de ctrllt
# Qué se prueba: ctrllt recibe la petición, la reenvía a gesfich y retorna la respuesta
prueba "Crear fichero VIA ctrllt (ctrllt enruta a gesfich)" \
    '{"servicio":"gesfich","operacion":"Crear"}' \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# PRUEBA 3.2: Guardar programa a través de ctrllt
prueba "Guardar programa VIA ctrllt (ctrllt enruta a gesprog)" \
    '{"servicio":"gesprog","operacion":"Guardar","ejecutable":"/usr/bin/wc","args":["-l"]}' \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# PRUEBA 3.3: Servicio desconocido
# Qué se prueba: ctrllt retorna error cuando el campo "servicio" es inválido
prueba "Servicio desconocido (ctrllt debe retornar error)" \
    '{"servicio":"servicio_que_no_existe","operacion":"Crear"}' \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# =============================================================================
# SECCIÓN 4: PRUEBA DE EJECUCIÓN DE PROCESO DE LOTES
# =============================================================================

titulo "SECCIÓN 4: Prueba de proceso de lotes completo"

echo ""
echo "Para esta prueba se usa:"
echo "  - Programa: /usr/bin/sort (registrado como p-0001 si se reiniciaron los servicios)"
echo "  - Fichero stdin: f-0001 (con datos de prueba cargados en la sección 1)"
echo "  - Fichero stdout: un nuevo fichero f-XXXX creado ahora"
echo ""

# Crear fichero de salida para el proceso de lotes
echo -e "${VERDE}▶ Creando fichero de salida para el proceso de lotes...${RESET}"
echo '{"servicio":"gesfich","operacion":"Crear"}' > "$PIPE_CLI_REQ" &
sleep "$ESPERA"
ID_SALIDA=$(timeout 2 head -n 1 "$PIPE_CLI_RES" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id-fichero','f-error'))" 2>/dev/null || echo "f-0003")
echo "  ID fichero de salida: $ID_SALIDA"

# Crear datos de entrada para el proceso de lotes
echo -e "zebra\nmanzana\narbol\nbanana" > /tmp/datos_sort.txt

# Actualizar el fichero de entrada con los datos
prueba "Cargar datos de entrada en f-0001 para el sort" \
    "{\"servicio\":\"gesfich\",\"operacion\":\"Actualizar\",\"id-fichero\":\"f-0001\",\"ruta\":\"/tmp/datos_sort.txt\"}" \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# Verificar que p-0001 existe (sort fue registrado en la sección 2)
# Si no existe, registrarlo ahora
prueba "Verificar/registrar programa sort como p-0001" \
    '{"servicio":"gesprog","operacion":"Leer","id-programa":"p-0001"}' \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# PRUEBA 4.1: Ejecutar proceso de lotes
# Qué se prueba: ejecutor lanza /usr/bin/sort con f-0001 como stdin y un fichero como stdout
echo ""
echo -e "${VERDE}▶ Ejecutando proceso de lotes: sort de f-0001 → $ID_SALIDA${RESET}"
PETICION_EJECUTAR="{\"servicio\":\"ejecutor\",\"operacion\":\"Ejecutar\",\"id-programa\":\"p-0001\",\"stdin\":\"f-0001\",\"stdout\":\"$ID_SALIDA\"}"
echo "  Enviando: $PETICION_EJECUTAR"
echo "$PETICION_EJECUTAR" > "$PIPE_CLI_REQ" &
sleep "$ESPERA"
RESP_EJEC=$(timeout 2 head -n 1 "$PIPE_CLI_RES" 2>/dev/null || echo '{"estado":"error"}')
echo "  Respuesta: $RESP_EJEC"
ID_EJEC=$(echo "$RESP_EJEC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('id-ejecucion','e-error'))" 2>/dev/null || echo "e-0001")
echo "  ID ejecución: $ID_EJEC"

# Esperar a que el proceso de lotes termine
sleep 1

# PRUEBA 4.2: Consultar estado del proceso de lotes
prueba "Consultar estado de $ID_EJEC (debe mostrar Terminado con codigo-salida)" \
    "{\"servicio\":\"ejecutor\",\"operacion\":\"Estado\",\"id-ejecucion\":\"$ID_EJEC\"}" \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# PRUEBA 4.3: Leer el resultado del sort
prueba "Leer resultado del sort en $ID_SALIDA (debe estar ordenado alfabéticamente)" \
    "{\"servicio\":\"gesfich\",\"operacion\":\"Leer\",\"id-fichero\":\"$ID_SALIDA\"}" \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# PRUEBA 4.4: Consultar todos los procesos de lotes
prueba "Listar todos los procesos de lotes" \
    '{"servicio":"ejecutor","operacion":"Estado"}' \
    "$PIPE_CLI_REQ" "$PIPE_CLI_RES"

# =============================================================================
# SECCIÓN 5: APAGADO DEL SISTEMA
# =============================================================================

titulo "SECCIÓN 5: Apagado ordenado del sistema"

# PRUEBA 5.1: Terminar todo el sistema a través de ctrllt
# Qué se prueba: ctrllt envía Terminar a gesfich y gesprog, Parar a ejecutor,
# y luego sale él mismo
echo ""
read -p "¿Apagar todo el sistema? Esto enviará Terminar a ctrllt. (s/N) " confirm
if [[ "$confirm" == "s" || "$confirm" == "S" ]]; then
    prueba "Terminar el sistema completo (ctrllt apaga todos los servicios)" \
        '{"servicio":"ctrllt","operacion":"Terminar"}' \
        "$PIPE_CLI_REQ" "$PIPE_CLI_RES"
    echo ""
    echo -e "${VERDE}✓ Sistema apagado. Verifica que los procesos de los servicios hayan terminado.${RESET}"
else
    echo "Apagado cancelado. Los servicios siguen corriendo."
fi

# =============================================================================
# RESUMEN
# =============================================================================

titulo "RESUMEN DE PRUEBAS"

echo ""
echo "Pruebas completadas. Resultados esperados:"
echo ""
echo "  ✓ Sección 1 (gesfich):"
echo "      - Crear retorna f-0001, f-0002"
echo "      - Leer lista los ficheros existentes"
echo "      - Actualizar carga contenido externo"
echo "      - Leer por ID retorna el contenido"
echo "      - Borrar elimina el fichero"
echo "      - Suspender/Reasumir controla el estado"
echo ""
echo "  ✓ Sección 2 (gesprog):"
echo "      - Guardar valida el ejecutable y retorna p-0001"
echo "      - Leer en Suspendido FUNCIONA (diferencia vs gesfich)"
echo ""
echo "  ✓ Sección 3 (ctrllt):"
echo "      - Peticiones se enrutan al servicio correcto"
echo "      - Servicio desconocido retorna error"
echo ""
echo "  ✓ Sección 4 (ejecutor):"
echo "      - Lanza proceso de lotes y retorna id-ejecucion"
echo "      - Estado retorna Terminado con codigo-salida"
echo "      - El resultado del sort aparece en el fichero de salida"
echo ""
echo "Archivos de prueba en: $ARALMAC"
echo "Para limpiar: rm -rf $ARALMAC /tmp/ejlotes_*.* /tmp/datos_*.txt"
