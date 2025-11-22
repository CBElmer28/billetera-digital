"""Funciones de utilidad para el Ledger Service, principalmente carga de configuración."""

import os
import logging
from dotenv import load_dotenv

# Configuración del logger
# (Asegúrate que el logger principal en main.py se configure primero si es necesario)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env_vars():
    """
    Carga las variables de entorno desde un archivo .env y verifica que las variables
    esenciales para el funcionamiento del servicio estén definidas. Lanza un error si falta alguna.
    """
    load_dotenv()

    # Lista de variables de entorno críticas para el servicio
    required_vars = [
        "BALANCE_SERVICE_URL",
        "INTERBANK_SERVICE_URL", # Renombrado desde MOCK_BANKB_URL
        "CASSANDRA_HOST",
        "INTERBANK_API_KEY" # Añadida la clave API
    ]

    missing = [var for var in required_vars if not os.getenv(var)]

    if missing:
        msg = f"Error Crítico: Faltan variables de entorno esenciales: {', '.join(missing)}"
        logger.critical(msg)
        # Detiene la ejecución si faltan variables críticas
        raise EnvironmentError(msg)
    else:
        logger.info("Variables de entorno cargadas y verificadas correctamente.")

# Puedes añadir más funciones de utilidad aquí si son necesarias para el ledger_service.