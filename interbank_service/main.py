"""Servicio FastAPI que simula la API de un banco externo (Happy Money) para recibir transferencias interbancarias."""

import uuid
import os
import logging
import time
from typing import Dict # Para type hint

from fastapi import FastAPI, HTTPException, status, Header, Depends, Request, Response
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Importaciones locales (absolutas)
import schemas

# Carga variables de entorno (para EXPECTED_API_KEY)
load_dotenv()

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lee la API Key esperada desde el entorno
EXPECTED_API_KEY = os.getenv("EXPECTED_API_KEY")
if not EXPECTED_API_KEY:
    logger.warning("EXPECTED_API_KEY no definida. Usando valor inseguro por defecto para desarrollo.")
    EXPECTED_API_KEY = "happy-money-secret-key-for-dev" # Solo para desarrollo

# Inicializa FastAPI
app = FastAPI(
    title="Interbank Service (Simulador Happy Money)",
    description="Simula la API de Happy Money para recibir transferencias BDI-BDI.",
    version="1.0.0"
)

# --- Métricas Prometheus ---
REQUEST_COUNT = Counter(
    "interbank_requests_total",
    "Total requests processed by Interbank Service",
    ["method", "endpoint", "status_code"]
)
REQUEST_LATENCY = Histogram(
    "interbank_request_latency_seconds",
    "Request latency in seconds for Interbank Service",
    ["endpoint"]
)

# --- Middleware para Métricas ---
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = None
    status_code = 500 # Default

    try:
        response = await call_next(request)
        status_code = response.status_code
    except HTTPException as http_exc:
        status_code = http_exc.status_code
        raise http_exc
    except Exception as exc:
        logger.error(f"Middleware error: {exc}", exc_info=True)
        return Response("Internal Server Error", status_code=500)
    finally:
        latency = time.time() - start_time
        endpoint = request.url.path
        final_status_code = getattr(response, 'status_code', status_code)
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=final_status_code
        ).inc()

    return response

# --- Dependencia para verificar API Key ---
async def verify_api_key(x_api_key: str = Header(..., description="Clave API secreta del banco origen.")):
    """Verifica que la cabecera X-API-KEY sea la esperada."""
    if x_api_key != EXPECTED_API_KEY:
        logger.warning(f"Intento de acceso interbancario con API Key inválida: {x_api_key}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API Key inválida o faltante."
        )
    return x_api_key # No necesitamos el valor, pero la dependencia debe devolver algo

# --- Endpoints de la API ---

@app.post("/interbank/transfers",
          status_code=status.HTTP_200_OK,
          tags=["Interbank Transfers"],
          dependencies=[Depends(verify_api_key)]) # Protegemos el endpoint con la API Key
async def receive_interbank_transfer(payload: schemas.InterbankTransferRequest) -> Dict:
    """
    Endpoint principal para recibir transferencias interbancarias (BDI -> BDI).
    Simula validaciones y procesamiento del banco Happy Money.
    """
    logger.info(f"Recibida transferencia interbancaria de {payload.origin_bank} para {payload.destination_phone_number} por {payload.amount} {payload.currency}. Tx ID Origen: {payload.transaction_id}")

    # Validación 1: ¿Es para nosotros?
    if payload.destination_bank.upper() != "HAPPY_MONEY":
        logger.warning(f"Transferencia rechazada: Destino incorrecto '{payload.destination_bank}'.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status": "REJECTED",
                "error_code": "INVALID_DESTINATION_BANK",
                "message": f"Banco destino '{payload.destination_bank}' no es HAPPY_MONEY."
            }
        )

    # Validación 2: Límite de monto (simulación)
    if payload.amount > 10000:
        logger.warning(f"Transferencia rechazada (Tx ID: {payload.transaction_id}): Límite de monto excedido ({payload.amount}).")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "status": "REJECTED",
                "error_code": "AMOUNT_LIMIT_EXCEEDED",
                "message": "Límite de transferencia externa excedido ($10,000)."
            }
        )

    # Validación 3: Cuenta/Teléfono destino (simulación)
    # Simulamos que algunos números no existen o están bloqueados
    if payload.destination_phone_number.startswith("999"):
        logger.warning(f"Transferencia rechazada (Tx ID: {payload.transaction_id}): Cuenta/Teléfono destino no encontrado ({payload.destination_phone_number}).")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "status": "REJECTED",
                "error_code": "ACCOUNT_NOT_FOUND",
                "message": f"Número de celular destino '{payload.destination_phone_number}' no encontrado en Happy Money."
            }
        )
    if payload.destination_phone_number.startswith("988"):
        logger.warning(f"Transferencia rechazada (Tx ID: {payload.transaction_id}): Cuenta/Teléfono destino bloqueado ({payload.destination_phone_number}).")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "status": "REJECTED",
                "error_code": "ACCOUNT_BLOCKED",
                "message": f"Cuenta destino '{payload.destination_phone_number}' está bloqueada."
            }
        )

    # Si pasa todas las validaciones, simulamos aceptación
    remote_tx_id = f"HAPPY-{uuid.uuid4()}" # Generamos un ID de transacción local
    logger.info(f"Transferencia ACEPTADA (Tx ID Origen: {payload.transaction_id}, Tx ID Happy: {remote_tx_id}). Acreditando a {payload.destination_phone_number}...")

    # En un sistema real, aquí se iniciaría el proceso de acreditación al usuario destino.

    return {
        "status": "ACCEPTED", # O "COMPLETED" si fuera instantáneo
        "remote_transaction_id": remote_tx_id,
        "message": "Transferencia recibida y aceptada por Happy Money."
    }

# --- Endpoint de Salud y Métricas ---
@app.get("/health", tags=["Monitoring"])
def health_check():
    """Verifica la salud básica del servicio."""
    return {"status": "ok", "service": "interbank_service"}

@app.get("/metrics", tags=["Monitoring"])
def metrics():
    """Expone métricas de la aplicación para Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)