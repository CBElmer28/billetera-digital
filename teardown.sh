#!/bin/bash
# teardown.sh - Detiene y limpia completamente el entorno Docker de Pixel Money

echo "Iniciando limpieza del entorno..."
set -e

# 1. Detener y eliminar todos los contenedores definidos en docker-compose.yml
#    -v elimina los volúmenes anónimos asociados
#    --remove-orphans elimina contenedores creados por builds anteriores si ya no están definidos
echo "Deteniendo y eliminando contenedores..."
docker compose down -v --remove-orphans

# 2. Eliminar volúmenes nombrados explícitamente (¡BORRA TODOS LOS DATOS!)
#    Esto asegura una limpieza total, incluyendo bases de datos.
echo "Eliminando volúmenes nombrados (MariaDB, Cassandra, Grafana, n8n)..."
# Lista los volúmenes del proyecto (asumiendo prefijo por defecto del directorio) y los elimina
# Adaptar el filtro si usas un nombre de proyecto diferente con `docker compose -p <nombre>`
docker volume rm $(docker volume ls -q --filter name=billetera-digital_*) 2>/dev/null || echo "No se encontraron volúmenes nombrados para eliminar o ya estaban eliminados."



echo "Entorno completamente limpio."