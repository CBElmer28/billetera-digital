#!/bin/bash
# deploy.sh - Despliega la infraestructura completa de la Billetera Digital Pixel Money

echo "Iniciando despliegue para Pixel Money..."
# Detiene el script si cualquier comando falla
set -e 

# 1. Verificar Docker y Docker Compose (v2+)
if ! command -v docker &> /dev/null; then
    echo "Error: Docker no está instalado o no se encuentra en el PATH."
    exit 1
fi
# Usamos 'docker compose' (v2) en lugar del antiguo 'docker-compose' (v1)
if ! docker compose version &> /dev/null; then
     echo "Error: Docker Compose v2 no está instalado o no funciona. Asegúrate de tener Docker Desktop actualizado."
     exit 1
fi
echo "Docker y Docker Compose v2 verificados."

# 2. Construir imágenes (si hay cambios en Dockerfile o código fuente)
echo "Construyendo/Actualizando imágenes de los servicios..."
docker compose build --pull # --pull intenta actualizar imágenes base como python:3.11-slim

# 3. Levantar todos los servicios en segundo plano
echo "Levantando todos los servicios (-d)..."
docker compose up -d

# 4. Mostrar estado final
echo "Verificando estado de los contenedores..."
# Espera breve para que los contenedores se estabilicen
sleep 15 
docker compose ps --format "table {{.Name}}\t{{.State}}\t{{.Status}}\t{{.Ports}}"

echo "Despliegue completado."
echo "---"
echo "Interfaces Web Disponibles:"
echo "  - API Gateway (Swagger): http://localhost:8080/docs"
echo "  - MailHog (Correos):    http://localhost:8025"
echo "  - n8n Dashboard:        http://localhost:5678 (user: admin, pass: admin)"
echo "  - Prometheus Targets:   http://localhost:9090/targets"
echo "  - Alertmanager:         http://localhost:9093"
echo "  - Grafana:              http://localhost:3000 (user: admin, pass: admin)"
echo "---"
echo "Recuerda que los servicios pueden tardar un poco en estar completamente 'healthy'."