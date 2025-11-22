"""
Script Watchdog que monitorea el estado de contenedores Docker clave
y intenta reiniciarlos automáticamente si no están saludables o corriendo.
Notifica los eventos (reinicio exitoso/fallido, error) a un webhook de n8n.
"""

import docker
import requests
import time
import os
import logging
from datetime import datetime # Importar datetime
from dotenv import load_dotenv

# Carga variables de .env si existen (útil para pruebas locales fuera de Docker)
load_dotenv()

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] [%(levelname)s] %(message)s')
logger = logging.getLogger("Watchdog")

# --- Configuración Leída del Entorno ---
# URL del webhook en n8n para recibir notificaciones de recuperación/fallo.
N8N_ALERT_WEBHOOK = os.getenv("N8N_ALERT_WEBHOOK", "http://n8n:5678/webhook/recovery")
# Intervalo entre chequeos, en segundos.
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", 60))
# Nombres de los contenedores a monitorear (deben coincidir con docker-compose.yml).
MONITORED_CONTAINERS = [
    "gateway_service",
    "auth_service",
    "balance_service",
    "ledger_service",
    "group_service", 
    "interbank_service", 
    "n8n" 
]

# --- Conexión Inicial a Docker ---
docker_client = None
try:
    # Intenta conectarse al daemon de Docker usando el socket montado.
    docker_client = docker.from_env()
    # Verifica la conexión pidiendo la versión del servidor Docker.
    docker_version = docker_client.version()
    logger.info(f"Conectado exitosamente al Docker Engine (API versión: {docker_version.get('ApiVersion', 'N/A')}).")
except Exception as e:
    logger.critical(f"FATAL: No se pudo conectar al daemon de Docker via socket. Error: {e}", exc_info=True)
    # Sin conexión a Docker, el watchdog no puede operar.
    exit(1)

def check_containers():
    """
    Realiza un ciclo de verificación del estado de los contenedores monitoreados.
    Intenta reiniciar los que no estén 'running' o 'healthy'.
    """
    logger.info("Iniciando ciclo de verificación de contenedores...")
    for container_name in MONITORED_CONTAINERS:
        container = None # Resetear para cada contenedor
        try:
            container = docker_client.containers.get(container_name)
            container_status = container.status 
            
            # Obtenemos el estado de salud si el contenedor lo tiene definido.
            health_status = container.attrs.get("State", {}).get("Health", {}).get("Status")
            

            # Condición de fallo: No está corriendo O está explícitamente no saludable.
            is_unhealthy = container_status != "running" or health_status == "unhealthy"

            if is_unhealthy:
                logger.warning(f"⚠️ Contenedor '{container_name}' detectado en estado: {container_status} (Salud: {health_status or 'N/A'}). Intentando reiniciar...")
                try:
                    container.restart(timeout=30) # Intenta reiniciar, espera hasta 30s
                    logger.info(f"Contenedor '{container_name}' reiniciado exitosamente por Watchdog.")
                    send_alert(container_name, "reiniciado_por_watchdog", f"Estado anterior: {container_status}, Salud anterior: {health_status or 'N/A'}")
                except Exception as restart_err:
                    logger.error(f"Error al intentar reiniciar '{container_name}': {restart_err}", exc_info=True)
                    send_alert(container_name, "fallo_reinicio_watchdog", str(restart_err))
            

        except docker.errors.NotFound:
            # El contenedor no existe según Docker.
            logger.error(f"Contenedor '{container_name}' no encontrado. ¿Está definido correctamente en docker-compose.yml y desplegado?")
            # Notificamos que no se encontró, podría ser un error de configuración.
            send_alert(container_name, "no_encontrado_por_watchdog", "docker.errors.NotFound")
        except Exception as e:
            # Captura cualquier otro error durante la verificación de este contenedor.
            logger.error(f"❓ Error inesperado al verificar '{container_name}': {e}", exc_info=True)
            send_alert(container_name, "error_verificacion_watchdog", str(e))

def send_alert(container_name: str, action: str, detail: str = "N/A"):
    """
    Envía una notificación (payload JSON) al webhook configurado en n8n.

    Args:
        container_name: Nombre del contenedor afectado.
        action: Acción realizada o estado detectado (ej. 'reiniciado', 'fallo_reinicio', 'no_encontrado').
        detail: Información adicional sobre el evento.
    """
    payload = {
        "container": container_name,
        "action": action,
        "detail": detail,
        "timestamp": datetime.now().isoformat() # Marca de tiempo del evento
    }
    try:
        response = requests.post(N8N_ALERT_WEBHOOK, json=payload, timeout=10) # Timeout de 10s
        response.raise_for_status() # Lanza error si n8n devuelve 4xx o 5xx
        logger.info(f"Notificación enviada a n8n para '{container_name}' (Acción: {action})")
    except requests.exceptions.Timeout:
        logger.error(f"Timeout al enviar notificación a n8n para '{container_name}'.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al enviar notificación a n8n para '{container_name}': {e}", exc_info=True)

# --- Bucle Principal del Watchdog ---
if __name__ == "__main__":
    logger.info("--- [Watchdog Pixel Money] Iniciado ---")
    logger.info(f"Monitoreando contenedores: {', '.join(MONITORED_CONTAINERS)}")
    logger.info(f"Intervalo de chequeo: {CHECK_INTERVAL} segundos")
    logger.info(f"Notificando eventos a: {N8N_ALERT_WEBHOOK}")

    while True:
        try:
            check_containers()
            logger.info(f"Ciclo de verificación completado. Durmiendo por {CHECK_INTERVAL} segundos...")
            time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Interrupción recibida. Deteniendo Watchdog...")
            break
        except Exception as loop_error:
            # Captura errores inesperados en el bucle principal para evitar que el watchdog muera.
            logger.error(f"Error inesperado en el bucle principal del Watchdog: {loop_error}", exc_info=True)
            # Espera antes de reintentar para no saturar en caso de errores persistentes.
            time.sleep(CHECK_INTERVAL)

    logger.info("--- [Watchdog Pixel Money] Detenido ---")