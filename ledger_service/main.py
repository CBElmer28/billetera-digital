"""Servicio FastAPI para gestionar el registro de transacciones (Ledger) en Cassandra."""

import os
import httpx
import uuid
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from collections import defaultdict
from decimal import Decimal

from fastapi import FastAPI, Depends, HTTPException, status, Header, Request, Response
from cassandra.cluster import Session
from cassandra.query import SimpleStatement, BatchStatement
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
import time

# Importaciones locales (absolutas)
import cassandra_db
import schemas


try:
    from utils import load_env_vars
    load_env_vars() # Carga y verifica variables de entorno
except ImportError:
    from dotenv import load_dotenv
    load_dotenv()
    if 'logger' not in locals(): # Configura el logger si utils.py no lo hizo
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.warning("Archivo utils.py no encontrado, cargando .env directamente.")

BALANCE_SERVICE_URL = os.getenv("BALANCE_SERVICE_URL")
INTERBANK_SERVICE_URL = os.getenv("INTERBANK_SERVICE_URL")
INTERBANK_API_KEY = os.getenv("INTERBANK_API_KEY")
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL")
GROUP_SERVICE_URL = os.getenv("GROUP_SERVICE_URL") # ¡El que faltaba!
KEYSPACE = cassandra_db.KEYSPACE
CENTRAL_API_URL = os.getenv("CENTRAL_API_URL")
CENTRAL_WALLET_TOKEN = os.getenv("CENTRAL_WALLET_TOKEN")
APP_NAME = os.getenv("APP_NAME", "PIXEL MONEY")

# Configura logger (si no se hizo arriba)
if 'logger' not in locals():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

# --- Inicialización de la App y Base de Datos ---
app = FastAPI(
    title="Ledger Service - Pixel Money",
    description="Registra todas las transacciones financieras (depósitos, transferencias, aportes) en Cassandra.",
    version="1.0.0"
)
db_session: Optional[Session] = None

@app.on_event("startup")
def startup_event():
    global db_session
    logger.info("Iniciando Ledger Service...")
    db_session = cassandra_db.get_cassandra_session()
    if db_session:
        try:
            cassandra_db.create_keyspace_and_tables(db_session)
        except Exception as e:
            logger.critical(f"FATAL: Error al configurar schema de Cassandra: {e}. El servicio no funcionará.", exc_info=True)
            db_session = None # Marcamos la sesión como nula
    else:
        logger.critical("FATAL: No se pudo conectar a Cassandra al inicio. El servicio no funcionará.")

@app.on_event("shutdown")
def shutdown_event():
    if db_session and db_session.cluster:
        db_session.cluster.shutdown()
        logger.info("Conexión a Cassandra cerrada.")

def get_db() -> Session:
    if db_session is None:
        logger.error("Intento de acceso a BD fallido: Sesión de Cassandra no disponible.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Servicio de base de datos (Cassandra) no disponible temporalmente."
        )
    return db_session



# --- Métricas Prometheus (Corregido para coincidir con el PDF) ---
REQUEST_COUNT = Counter(
    "ledger_requests_total", 
    "Total requests", 
    ["method", "endpoint", "status_code"]
)
REQUEST_LATENCY = Histogram(
    "ledger_request_latency_seconds", 
    "Request latency", 
    ["endpoint"]
)
# ¡Nombres cortos que tu PDF usa!
DEPOSIT_COUNT = Counter(
    "ledger_deposits_total", 
    "Número total de depósitos procesados"
)
TRANSFER_COUNT = Counter(
    "ledger_transfers_total", 
    "Número total de transferencias procesadas"
)
CONTRIBUTION_COUNT = Counter(
    "ledger_contributions_total", 
    "Número total de aportes a grupos"
)
LEDGER_P2P_TRANSFERS_TOTAL = Counter(
    "ledger_p2p_transfers_total",
    "Total de transferencias P2P (BDI -> BDI) procesadas"
)
# ¡AQUÍ ESTÁ EL SYNTAX ERROR ARREGLADO!
LEDGER_WITHDRAWALS_TOTAL = Counter(
    "ledger_withdrawals_total",
    "Total de retiros (BDI -> Banco Externo) procesados"
)
# --- Fin del Bloque Corregido ---
# --- Middleware para Métricas ---
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = None
    status_code = 500
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
        REQUEST_COUNT.labels(method=request.method, endpoint=endpoint, status_code=final_status_code).inc()
    return response

# --- Funciones de Utilidad ---
def check_idempotency(session: Session, key: str) -> Optional[uuid.UUID]:
    if not key:
        return None
    try:
        key_uuid = uuid.UUID(key)
        query = SimpleStatement(f"SELECT transaction_id FROM {KEYSPACE}.idempotency_keys WHERE key = %s")
        result = session.execute(query, (key_uuid,)).one()
        return result.transaction_id if result else None
    except (ValueError, TypeError):
        logger.warning(f"Clave de idempotencia inválida recibida: {key}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de Idempotency-Key inválido (debe ser UUID)")
    except Exception as e:
        logger.error(f"Error al verificar idempotencia para key {key}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error interno al verificar idempotencia")

async def get_transaction_by_id(session: Session, tx_id: uuid.UUID) -> Optional[dict]:
    try:
        query = SimpleStatement(f"SELECT * FROM {KEYSPACE}.transactions WHERE id = %s")
        result = session.execute(query, (tx_id,)).one()
        
        if not result:
            return None
            
        # --- CORRECCIÓN: Validar si ya es dict ---
        return result if isinstance(result, dict) else result._asdict()
        
    except Exception as e:
        logger.error(f"Error al obtener transacción {tx_id}: {e}", exc_info=True)
        return None
# --- Endpoints de la API ---

@app.post("/deposit", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def deposit(
    req: schemas.DepositRequest,
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")

    existing_tx_id = check_idempotency(db, idempotency_key)
    if existing_tx_id:
        logger.info(f"Depósito duplicado (Key: {idempotency_key}). Devolviendo tx: {existing_tx_id}")
        tx_data = await get_transaction_by_id(db, existing_tx_id)
        if tx_data: return schemas.Transaction(**tx_data)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia: Tx original no encontrada")

    tx_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    metadata_json = json.dumps({"description": "Depósito en BDI"})
    status_final = "PENDING"
    currency = "PEN"

    try:
        # (El BATCH de PENDING... se queda igual que en el PDF) [cite: 168-187]
        query_by_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'EXTERNAL', 'N/A', 'BDI', %s, 'DEPOSIT', %s, %s, %s, %s, %s, %s)")
        query_by_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'EXTERNAL', 'N/A', 'BDI', %s, 'DEPOSIT', %s, %s, %s, %s, %s)")
        batch = BatchStatement()
        batch.add(query_by_id, (tx_id, req.user_id, str(req.user_id), Decimal(str(req.amount)), currency, status_final, now, now, metadata_json))
        batch.add(query_by_user, (req.user_id, now, tx_id, str(req.user_id), Decimal(str(req.amount)), currency, status_final, now, metadata_json))
        db.execute(batch)
    except Exception as e:
        logger.error(f"Error al insertar BATCH PENDING (depósito) {tx_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al registrar la transacción inicial")

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/credit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            response.raise_for_status()
        status_final = "COMPLETED"
    except (httpx.RequestError, httpx.HTTPStatusError) as e:
        # (La lógica de error de depósito... se queda igual que en el PDF) [cite: 199-212]
        status_final = "FAILED_BALANCE_SVC"
        detail = f"Balance Service falló al acreditar: {e}"
        status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        if isinstance(e, httpx.HTTPStatusError):
            try: 
                detail = e.response.json().get("detail", str(e))
            except json.JSONDecodeError: 
                detail = e.response.text
            status_code = e.response.status_code
        logger.error(f"Fallo en tx {tx_id} (depósito): {detail}")
        db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, updated_at = %s WHERE id = %s", (status_final, datetime.now(timezone.utc), tx_id))
        db.execute(f"UPDATE {KEYSPACE}.transactions_by_user SET status = %s, updated_at = %s WHERE user_id = %s AND created_at = %s AND id = %s", (status_final, datetime.now(timezone.utc), req.user_id, now, tx_id)) # ¡Fix! Añadido update a transactions_by_user
        raise HTTPException(status_code=status_code, detail=detail)

    try:
        idempotency_uuid = uuid.UUID(idempotency_key)
        db.execute(f"INSERT INTO {KEYSPACE}.idempotency_keys (key, transaction_id) VALUES (%s, %s)", (idempotency_uuid, tx_id))
        db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, updated_at = %s WHERE id = %s", (status_final, datetime.now(timezone.utc), tx_id))
        db.execute(f"UPDATE {KEYSPACE}.transactions_by_user SET status = %s, updated_at = %s WHERE user_id = %s AND created_at = %s AND id = %s", (status_final, datetime.now(timezone.utc), req.user_id, now, tx_id)) # ¡Fix! Añadido update

        DEPOSIT_COUNT.inc() 

        logger.info(f"Depósito {status_final} para user_id {req.user_id}, tx_id {tx_id}")
    except Exception as final_e:
        # (Lógica de PENDING_CONFIRMATION... se queda igual que en el PDF) [cite: 220-224]
        status_final = "PENDING_CONFIRMATION"
        db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, updated_at = %s WHERE id = %s", (status_final, datetime.now(timezone.utc), tx_id))
        db.execute(f"UPDATE {KEYSPACE}.transactions_by_user SET status = %s, updated_at = %s WHERE user_id = %s AND created_at = %s AND id = %s", (status_final, datetime.now(timezone.utc), req.user_id, now, tx_id)) # ¡Fix! Añadido update
        logger.critical(f"¡FALLO CRÍTICO post-crédito en tx {tx_id}! Estado: {status_final}. Error: {final_e}. Requiere reconciliación manual.")

    tx_data = await get_transaction_by_id(db, tx_id)
    if not tx_data: raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "No se pudo recuperar la transacción final")
    return schemas.Transaction(**tx_data)

@app.post("/transfer", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def transfer(
    req: schemas.TransferRequest, 
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    """Procesa una transferencia BDI -> BDI (Externa a Happy Money)."""
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")
    if req.to_bank.upper() != "HAPPY_MONEY":
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Banco de destino '{req.to_bank}' no soportado")

    existing_tx_id = check_idempotency(db, idempotency_key)
    if existing_tx_id:
        logger.info(f"Transferencia duplicada detectada (Key: {idempotency_key}). Devolviendo tx: {existing_tx_id}")
        tx_data = await get_transaction_by_id(db, existing_tx_id)
        if tx_data: return schemas.Transaction(**tx_data)
        logger.error(f"INCONSISTENCIA: Key {idempotency_key} existe pero tx_id {existing_tx_id} no encontrado.")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia: Transacción original no encontrada")

    tx_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    
    metadata = {"to_bank": req.to_bank, "destination_phone_number": req.destination_phone_number}
    status_final = "PENDING"
    currency = "PEN"

        # En la función transfer(), reemplaza el primer 'try...'
    try:
        query_by_id = SimpleStatement(f"""
            INSERT INTO {KEYSPACE}.transactions (
                id, user_id, source_wallet_type, source_wallet_id,
                destination_wallet_type, destination_wallet_id, type, amount, currency,
                status, created_at, updated_at, metadata
            ) VALUES (%s, %s, 'BDI', %s, 'EXTERNAL_BANK', %s, 'TRANSFER', %s, %s, %s, %s, %s, %s)
        """)

        query_by_user = SimpleStatement(f"""
            INSERT INTO {KEYSPACE}.transactions_by_user (
                user_id, created_at, id, source_wallet_type, source_wallet_id,
                destination_wallet_type, destination_wallet_id, type, amount, currency,
                status, updated_at, metadata
            ) VALUES (%s, %s, %s, 'BDI', %s, 'EXTERNAL_BANK', %s, 'TRANSFER', %s, %s, %s, %s, %s)
        """)

        batch = BatchStatement()
        batch.add(query_by_id, (tx_id, req.user_id, str(req.user_id), req.destination_phone_number, req.amount, currency, status_final, now, now, json.dumps(metadata)))
        batch.add(query_by_user, (req.user_id, now, tx_id, str(req.user_id), req.destination_phone_number, req.amount, currency, status_final, now, json.dumps(metadata)))

        db.execute(batch)

    except Exception as e:
        logger.error(f"Error al insertar BATCH PENDING (transfer) {tx_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al registrar la transacción inicial")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # 1. Verificar Fondos en BDI origen
            logger.debug(f"Tx {tx_id}: Verificando fondos para user_id {req.user_id}")
            check_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/check",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            # ¡Si esto falla (400), saltará al 'except HTTPStatusError'
            check_res.raise_for_status() 
            logger.debug(f"Tx {tx_id}: Fondos verificados.")

            # 2. Llamar al Servicio Interbancario (Happy Money)
            logger.debug(f"Tx {tx_id}: Llamando a Interbank Service...")
            interbank_payload = {
                "origin_bank": "PIXEL_MONEY",
                "origin_account_id": str(req.user_id),
                "destination_bank": req.to_bank.upper(),
                "destination_phone_number": req.destination_phone_number,
                "amount": req.amount,
                "currency": currency,
                "transaction_id": str(tx_id),
                "description": "Transferencia desde Pixel Money"
            }
            interbank_headers = {"X-API-KEY": INTERBANK_API_KEY}

            response_bank_b = await client.post(
                f"{INTERBANK_SERVICE_URL}/interbank/transfers",
                json=interbank_payload,
                headers=interbank_headers
            )

            # ¡Si el banco externo falla, raise_for_status() también saltará!
            response_bank_b.raise_for_status() 

            bank_b_response = response_bank_b.json()
            remote_tx_id = bank_b_response.get("remote_transaction_id")
            metadata["remote_tx_id"] = remote_tx_id
            logger.info(f"Banco externo aceptó tx {tx_id}. ID remoto: {remote_tx_id}")

            # 3. Debitar Saldo en BDI origen (Paso final)
            logger.debug(f"Tx {tx_id}: Debitando saldo de user_id {req.user_id}")
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            debit_res.raise_for_status() # Si el débito falla, saltará

            # 4. Todo OK
            status_final = "COMPLETED"

    # --- INICIO DEL BLOQUE CORREGIDO ---
    except httpx.HTTPStatusError as e:
        
        status_code = e.response.status_code
        try:
            detail = e.response.json().get("detail", "Error desconocido del servicio interno.")
        except json.JSONDecodeError:
            detail = e.response.text

        if status_code == 400: status_final = "FAILED_FUNDS" # Asumimos que 400 es Fondos Insuficientes
        elif status_code == 404: status_final = "FAILED_ACCOUNT"
        else: status_final = f"FAILED_HTTP_{status_code}" # Otro error (ej. 401 de API Key)

        logger.warning(f"Transferencia {status_final} para tx {tx_id}: {detail}")
        db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, metadata = %s, updated_at = %s WHERE id = %s",
                (status_final, json.dumps(metadata), datetime.now(timezone.utc), tx_id))
        # Re-lanzamos la excepción para que el cliente reciba el código y detalle correctos
        raise HTTPException(status_code=status_code, detail=detail)

    except httpx.RequestError as e: # Error de Red (timeout, servicio caído)
        status_final = "FAILED_NETWORK"
        db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, metadata = %s, updated_at = %s WHERE id = %s",
                (status_final, json.dumps(metadata), datetime.now(timezone.utc), tx_id))
        logger.error(f"Fallo de red en tx {tx_id} (transferencia): {e}", exc_info=True)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"Error de red al contactar servicios: {e}")

    except Exception as e: # Bug nuestro
        status_final = "FAILED_UNKNOWN"
        db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, metadata = %s, updated_at = %s WHERE id = %s",
                (status_final, json.dumps(metadata), datetime.now(timezone.utc), tx_id))
        logger.error(f"Error inesperado en tx {tx_id} (transferencia): {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno inesperado procesando la transferencia")
    

    # Si todo fue exitoso
    if status_final == "COMPLETED":
        try:
            idempotency_uuid = uuid.UUID(idempotency_key)
            db.execute(f"INSERT INTO {KEYSPACE}.idempotency_keys (key, transaction_id) VALUES (%s, %s)",
                       (idempotency_uuid, tx_id))
            db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, metadata = %s, updated_at = %s WHERE id = %s",
                       (status_final, json.dumps(metadata), datetime.now(timezone.utc), tx_id))
            LEDGER_P2P_TRANSFERS_TOTAL.inc() # Incrementamos métrica
            logger.info(f"Transferencia {status_final} para user_id {req.user_id}, tx_id {tx_id}")
        except Exception as final_e:
             status_final = "PENDING_CONFIRMATION"
             db.execute(f"UPDATE {KEYSPACE}.transactions SET status = %s, metadata = %s, updated_at = %s WHERE id = %s",
                   (status_final, json.dumps(metadata), datetime.now(timezone.utc), tx_id))
             logger.critical(f"¡FALLO CRÍTICO post-débito en tx {tx_id}! Estado: {status_final}. Error: {final_e}. Requiere reconciliación.")

    tx_data = await get_transaction_by_id(db, tx_id)
    if not tx_data: raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "No se pudo recuperar la transacción final")
    return schemas.Transaction(**tx_data)


# REEMPLAZA la función 'contribute_to_group' entera con esto:

@app.post("/contribute", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def contribute_to_group(
    req: schemas.ContributionRequest,
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    """
    Procesa un aporte desde una BDI (individual) a una BDG (grupal).
    Crea 2 transacciones: SENT (para el usuario) y RECEIVED (para el grupo).
    """
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")

    sender_id = req.user_id
    group_id = req.group_id
    amount = req.amount

    existing_tx_id = check_idempotency(db, idempotency_key)
    if existing_tx_id:
        logger.info(f"Aporte duplicado (Key: {idempotency_key}). Devolviendo tx: {existing_tx_id}")
        tx_data = await get_transaction_by_id(db, existing_tx_id)
        if tx_data: return schemas.Transaction(**tx_data)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia: Tx original no encontrada")

    tx_id_sent = uuid.uuid4()
    tx_id_received = uuid.uuid4()
    now = datetime.now(timezone.utc)
    currency = "PEN"
    metadata = {"contribution_to_group_id": group_id}
    metadata_json = json.dumps(metadata)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:

            # 1. Debitar BDI origen (¡Verifica y resta!)
            logger.debug(f"Tx {tx_id_sent}: Debitando BDI para user_id {sender_id}")
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": sender_id, "amount": amount}
            )
            debit_res.raise_for_status() # Falla aquí si hay 'Insufficient funds' (400)

            # 2. Acreditar BDG destino
            try:
                logger.debug(f"Tx {tx_id_received}: Acreditando BDG para group_id {group_id}")
                credit_res = await client.post(
                    f"{BALANCE_SERVICE_URL}/group_balance/credit",
                    json={"group_id": group_id, "amount": amount}
                )
                credit_res.raise_for_status() 

                # 3. Actualizar Saldo Interno
                logger.debug(f"Tx {tx_id_received}: Actualizando internal_balance para user {sender_id}")
                internal_res = await client.post(
                    f"{GROUP_SERVICE_URL}/groups/{group_id}/member_balance",
                    json={"user_id_to_update": sender_id, "amount": amount} # ¡Es un Aporte (positivo)!
                )
                internal_res.raise_for_status()

            except Exception as credit_error:
                # ¡FALLO DE SAGA! Revertir el débito
                logger.error(f"¡FALLO DE SAGA! Crédito al grupo {group_id} falló. Revertiendo débito {tx_id_sent}...")
                async with httpx.AsyncClient() as revert_client:
                    revert_res = await revert_client.post(
                        f"{BALANCE_SERVICE_URL}/balance/credit", # ¡Revertimos con un CRÉDITO!
                        json={"user_id": sender_id, "amount": amount}
                    )
                    revert_res.raise_for_status()
                logger.info(f"Reversión de débito BDI para tx {tx_id_sent} exitosa.")

                if isinstance(credit_error, httpx.HTTPStatusError):
                    raise HTTPException(status_code=credit_error.response.status_code, detail=f"Error al acreditar al grupo: {credit_error.response.json().get('detail')}")
                else:
                    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno al acreditar al grupo.")

        # 4. ¡ÉXITO! Escribir ambas transacciones en Cassandra
        status_final = "COMPLETED"
        decimal_amount = Decimal(str(amount))

        batch = BatchStatement()

        # Tx de SALIDA (para el historial del USUARIO)
        q_sent_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDI', %s, 'BDG', %s, 'CONTRIBUTION_SENT', %s, %s, %s, %s, %s, %s)")
        q_sent_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'BDI', %s, 'BDG', %s, 'CONTRIBUTION_SENT', %s, %s, %s, %s, %s)")
        batch.add(q_sent_id, (tx_id_sent, sender_id, str(sender_id), str(group_id), decimal_amount, currency, status_final, now, now, metadata_json))
        batch.add(q_sent_user, (sender_id, now, tx_id_sent, str(sender_id), str(group_id), decimal_amount, currency, status_final, now, metadata_json))

        # Tx de ENTRADA (para el historial del GRUPO)
        q_received_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDI', %s, 'BDG', %s, 'CONTRIBUTION_RECEIVED', %s, %s, %s, %s, %s, %s)")
        q_received_group = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_group (group_id, created_at, id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, %s, 'BDI', %s, 'BDG', %s, 'CONTRIBUTION_RECEIVED', %s, %s, %s, %s, %s)")
        batch.add(q_received_id, (tx_id_received, sender_id, str(sender_id), str(group_id), decimal_amount, currency, status_final, now, now, metadata_json))
        batch.add(q_received_group, (group_id, now, tx_id_received, sender_id, str(sender_id), str(group_id), decimal_amount, currency, status_final, now, metadata_json))

        db.execute(batch)
        db.execute(f"INSERT INTO {KEYSPACE}.idempotency_keys (key, transaction_id) VALUES (%s, %s)", (uuid.UUID(idempotency_key), tx_id_sent))

        CONTRIBUTION_COUNT.inc() 

        tx_data = await get_transaction_by_id(db, tx_id_sent)
        if not tx_data: raise Exception("No se pudo recuperar la transacción final")
        return schemas.Transaction(**tx_data)

    except httpx.HTTPStatusError as e: # Captura el 400 "Insufficient funds"
        status_code = e.response.status_code
        detail = e.response.json().get("detail", "Error en servicios internos.")
        status_final = "FAILED_FUNDS" if status_code == 400 else "FAILED_BALANCE_SVC"

        logger.warning(f"Aporte {status_final} para tx {tx_id_sent}: {detail}")
        # (Opcional: escribir una tx 'FAILED_FUNDS' en Cassandra)
        raise HTTPException(status_code=status_code, detail=detail)

    except Exception as e:
        logger.error(f"Error inesperado en tx {tx_id_sent} (aporte): {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno inesperado procesando el aporte")
    

@app.get("/transactions/me", response_model=List[schemas.Transaction], tags=["Ledger"])
async def get_my_transactions(
    x_user_id: int = Header(..., alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    logger.info(f"Obteniendo historial de movimientos para user_id: {x_user_id}")
    query = SimpleStatement(f"""
        SELECT * FROM {KEYSPACE}.transactions_by_user
        WHERE user_id = %s
        ORDER BY created_at DESC
        LIMIT 50
    """)
    try:
        result_set = db.execute(query, (x_user_id,))
        # --- CORRECCIÓN: Normalizar cada fila a diccionario ---
        transactions = []
        for row in result_set:
            row_data = row if isinstance(row, dict) else row._asdict()
            transactions.append(schemas.Transaction(**row_data))
        return transactions
    except Exception as e:
        logger.error(f"Error al obtener transacciones para user_id {x_user_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al obtener historial de movimientos.")

# ... (después de 'get_my_transactions')

@app.get("/transactions/group/{group_id}", response_model=List[schemas.Transaction], tags=["Ledger"])
async def get_group_transactions(
    group_id: int,
    db: Session = Depends(get_db)
):
    logger.info(f"Obteniendo historial de movimientos para group_id: {group_id}")
    query = SimpleStatement(f"""
        SELECT * FROM {KEYSPACE}.transactions_by_group
        WHERE group_id = %s
        ORDER BY created_at DESC
        LIMIT 100
    """)
    try:
        result_set = db.execute(query, (group_id,))
        # --- CORRECCIÓN: Normalizar cada fila a diccionario ---
        transactions = []
        for row in result_set:
            row_data = row if isinstance(row, dict) else row._asdict()
            transactions.append(schemas.Transaction(**row_data))
        return transactions
    except Exception as e:
        logger.error(f"Error al obtener transacciones para group_id {group_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al obtener historial de movimientos del grupo.")

@app.get("/analytics/daily_balance/{user_id}", tags=["Analytics"])
async def get_daily_balance(
    user_id: int, 
    db: Session = Depends(get_db)
):
    logger.info(f"Calculando saldo diario para user_id: {user_id}")
    now = datetime.now(timezone.utc)
    thirty_days_ago = now - timedelta(days=30)

    query = SimpleStatement(f"""
        SELECT created_at, type, amount
        FROM {KEYSPACE}.transactions_by_user
        WHERE user_id = %s
        AND created_at >= %s
        ORDER BY created_at ASC
    """)

    try:
        result_set = db.execute(query, (user_id, thirty_days_ago))

        daily_balance = defaultdict(Decimal)
        running_balance = Decimal('0.0') 

        for row in result_set:
            # --- CORRECCIÓN: Convertir a dict y usar corchetes ---
            r = row if isinstance(row, dict) else row._asdict()
            
            tx_date = r['created_at'].date() # Antes: row.created_at.date()
            tx_type = r['type']
            tx_amount = r['amount']

            if tx_type in ["DEPOSIT", "P2P_RECEIVED", "CONTRIBUTION_RECEIVED", "GROUP_WITHDRAWAL"]:
                running_balance += tx_amount
            elif tx_type in ["P2P_SENT", "CONTRIBUTION_SENT", "TRANSFER"]:
                running_balance -= tx_amount
            daily_balance[tx_date] = running_balance

        data = []
        balance = Decimal('0.0')
        for i in range(30, -1, -1):
            day = (now - timedelta(days=i)).date()
            if day in daily_balance:
                balance = daily_balance[day]

            data.append({
                "date": day.isoformat(),
                "balance": float(round(balance, 2))
            })

        return data
    except Exception as e:
        logger.error(f"Error al calcular balance diario para {user_id}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error al calcular el balance diario.")
    

@app.post("/transfer/p2p", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Transactions"])
async def transfer_p2p(
    req: schemas.P2PTransferRequest,
    idempotency_key: Optional[str] = Header(None, description="Clave única (UUID v4) para idempotencia"),
    db: Session = Depends(get_db)
):
    if idempotency_key is None:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Cabecera Idempotency-Key es requerida")

    sender_id = req.user_id 
    recipient_phone = req.destination_phone_number
    amount = req.amount

    existing_tx_id = check_idempotency(db, idempotency_key)
    if existing_tx_id:
        logger.info(f"Transferencia P2P duplicada. Devolviendo tx: {existing_tx_id}")
        tx_data = await get_transaction_by_id(db, existing_tx_id)
        if tx_data: return schemas.Transaction(**tx_data)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de idempotencia")

    tx_id_debit = uuid.uuid4()
    tx_id_credit = uuid.uuid4()
    now = datetime.now(timezone.utc)
    currency = "PEN"
    recipient_id = None
    
    # Variables para guardar nombres y mejorar el historial
    sender_name = "Usuario Pixel" 
    recipient_name = "Usuario Pixel"

    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # 1. Obtener datos del REMITENTE (Para guardarlos en el historial del destino)
            # Esto es lo nuevo: Buscamos quién envía para que el otro sepa quién fue
            try:
                sender_res = await client.get(f"{AUTH_SERVICE_URL}/users/{sender_id}")
                if sender_res.status_code == 200:
                    sender_data = sender_res.json()
                    sender_name = sender_data.get("name", "Usuario Pixel")
            except Exception as e:
                logger.warning(f"No se pudo obtener nombre del sender: {e}")

            # 2. Resolver DESTINATARIO
            logger.debug(f"Tx {tx_id_debit}: Buscando destinatario: {recipient_phone}")
            auth_res = await client.get(f"{AUTH_SERVICE_URL}/users/by-phone/{recipient_phone}")
            auth_res.raise_for_status()
            
            recipient_data = auth_res.json()
            recipient_id = int(recipient_data["id"])
            recipient_name = recipient_data.get("name", "Usuario Destino")

            if recipient_id == sender_id:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "No puedes transferirte a ti mismo.")

            # 3. Verificar y Debitar Remitente
            check_res = await client.post(f"{BALANCE_SERVICE_URL}/balance/check", json={"user_id": sender_id, "amount": amount})
            check_res.raise_for_status()

            debit_res = await client.post(f"{BALANCE_SERVICE_URL}/balance/debit", json={"user_id": sender_id, "amount": amount})
            debit_res.raise_for_status()

            # 4. Acreditar Destinatario
            try:
                credit_res = await client.post(f"{BALANCE_SERVICE_URL}/balance/credit", json={"user_id": recipient_id, "amount": amount})
                credit_res.raise_for_status()
            except Exception:
                # Reversión si falla crédito
                await client.post(f"{BALANCE_SERVICE_URL}/balance/credit", json={"user_id": sender_id, "amount": amount})
                raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Fallo destino. Revertido.")

        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            detail = e.response.json().get("detail", "Error interno.")
            raise HTTPException(status_code=status_code, detail=detail)
        except httpx.RequestError:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Error de comunicación.")

    # --- PASO FINAL: Guardar en Cassandra con METADATOS ---
    try:
        batch = BatchStatement()
        
        # Metadatos enriquecidos
        meta_sent = json.dumps({"destination_phone": recipient_phone, "destination_name": recipient_name})
        
        # ¡AQUÍ ESTÁ EL ARREGLO! Guardamos el sender_name en el historial del que recibe
        meta_received = json.dumps({"source_phone": "Confidencial", "sender_name": sender_name})

        # 1. SENT (Remitente)
        q_sent_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDI', %s, 'BDI', %s, 'P2P_SENT', %s, %s, 'COMPLETED', %s, %s, %s)")
        q_sent_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'BDI', %s, 'BDI', %s, 'P2P_SENT', %s, %s, 'COMPLETED', %s, %s)")
        
        batch.add(q_sent_id, (tx_id_debit, sender_id, str(sender_id), str(recipient_id), amount, currency, now, now, meta_sent))
        batch.add(q_sent_user, (sender_id, now, tx_id_debit, str(sender_id), str(recipient_id), amount, currency, now, meta_sent))

        # 2. RECEIVED (Destinatario)
        q_recv_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDI', %s, 'BDI', %s, 'P2P_RECEIVED', %s, %s, 'COMPLETED', %s, %s, %s)")
        q_recv_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'BDI', %s, 'BDI', %s, 'P2P_RECEIVED', %s, %s, 'COMPLETED', %s, %s)")
        
        batch.add(q_recv_id, (tx_id_credit, recipient_id, str(sender_id), str(recipient_id), amount, currency, now, now, meta_received))
        batch.add(q_recv_user, (recipient_id, now, tx_id_credit, str(sender_id), str(recipient_id), amount, currency, now, meta_received))

        db.execute(batch)
        db.execute(f"INSERT INTO {KEYSPACE}.idempotency_keys (key, transaction_id) VALUES (%s, %s)", (uuid.UUID(idempotency_key), tx_id_debit))
        LEDGER_P2P_TRANSFERS_TOTAL.inc()

        tx_data = await get_transaction_by_id(db, tx_id_debit)
        return schemas.Transaction(**tx_data)

    except Exception as e:
        logger.critical(f"Error Cassandra: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error guardando historial.")


@app.post("/transfers/inbound-central", status_code=200, tags=["Central API"])
async def process_central_deposit(payload: dict, db: Session = Depends(get_db)):
    """
    WEBHOOK DE RECEPCIÓN (Inbound):
    Recibe notificaciones de dinero entrante desde la API Central.
    Flujo:
    1. Validar payload (seguridad y datos).
    2. Acreditar saldo al usuario local (Balance Service).
    3. Registrar transacción en Cassandra.
    4. Confirmar éxito a la Central.
    """
    logger.info(f"Webhook Central recibido. Payload: {payload}")

    # --- PASO 1: Extracción y Validación de Datos ---
    try:
        # La Central envía 'internalWalletId' como string, pero tu sistema usa int.
        # Si falla la conversión, es un intento inválido.
        internal_user_id = int(payload.get("internalWalletId")) 
        
        amount = float(payload.get("monto"))
        if amount <= 0:
            raise ValueError("El monto debe ser positivo")

        central_tx_id = payload.get("centralTransactionId")

        namespace_uuid = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8') # Namespace DNS
        idempotency_key_uuid = uuid.uuid5(namespace_uuid, central_tx_id)

        existing_tx_id = check_idempotency(db, str(idempotency_key_uuid))
        if existing_tx_id:
            logger.info(f"Webhook duplicado (Central ID: {central_tx_id}). Ignorando.")
        # Importante: Devolver 200 OK y el ID existente para que la Central deje de reintentar
            return {
            "success": True,
            "localTransactionId": str(existing_tx_id)
            }



        from_app_name = payload.get("fromAppName", "CentralAPI")
        from_user_name = payload.get("fromUserName", "Desconocido")
        
        # ¡CRÍTICO! Validar que traiga un ID de transacción central para evitar duplicados
        if not central_tx_id:
            raise ValueError("Falta centralTransactionId")

    except (ValueError, TypeError) as e:
        logger.warning(f"Payload inválido en webhook central: {e}")
        # 400 le dice a la Central: "Tu petición está mal formada, no la reintentes"
        raise HTTPException(status_code=400, detail=f"Datos inválidos: {str(e)}")

    # --- PASO 2: Idempotencia (Evitar doble gasto) ---
    # Consultamos a Cassandra si ya procesamos este central_tx_id
    # (Nota: Esto requiere una tabla auxiliar o índice en Cassandra. 
    # Por simplicidad ahora, confiamos en que si la inserción final falla, no acreditamos doble)
    
    tx_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    currency = "PEN"
    
    # Metadatos para auditoría
    metadata = {
        "central_tx_id": central_tx_id,
        "from_app": from_app_name,
        "from_user": from_user_name,
        "description": payload.get("description", "Transferencia Interbancaria")
    }
    metadata_json = json.dumps(metadata)

    # --- PASO 3: SAGA - Acreditar Saldo (Balance Service) ---
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            # Llamamos a nuestro microservicio de Balance
            res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/credit",
                json={"user_id": internal_user_id, "amount": amount}
            )
            res.raise_for_status()
            logger.info(f"Saldo acreditado a usuario {internal_user_id} por {amount}")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                logger.error(f"Usuario destino {internal_user_id} no existe en Balance.")
                # 404 le dice a la Central: "El destinatario no existe, devuélvele la plata al remitente"
                raise HTTPException(status_code=404, detail="Usuario no encontrado")
            else:
                logger.error(f"Error interno Balance: {e.response.text}")
                # 500 le dice a la Central: "Tengo problemas, reintenta más tarde"
                raise HTTPException(status_code=503, detail="Error interno procesando saldo")
        except Exception as e:
            logger.error(f"Error de conexión Balance Service: {e}")
            raise HTTPException(status_code=503, detail="Error de conexión interno")

    db.execute(f"INSERT INTO {KEYSPACE}.idempotency_keys (key, transaction_id) VALUES (%s, %s)", (idempotency_key_uuid, tx_id))
    # --- PASO 4: Registrar en Historial (Cassandra) ---
    try:
        decimal_amount = Decimal(str(amount))
        status_final = "COMPLETED"
        
        batch = BatchStatement()
        
        # 1. Tabla Principal (Log global)
        q_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'CENTRAL_API', %s, 'BDI', %s, 'DEPOSIT', %s, %s, %s, %s, %s, %s)")
        
        # 2. Tabla por Usuario (Para que Pepe vea su historial)
        q_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'CENTRAL_API', %s, 'BDI', %s, 'DEPOSIT', %s, %s, %s, %s, %s)")

        # Source Wallet ID = Nombre de la App que envía (ej: "LUCA")
        # Destination Wallet ID = ID interno del usuario
        batch.add(q_id, (tx_id, internal_user_id, from_app_name, str(internal_user_id), decimal_amount, currency, status_final, now, now, metadata_json))
        batch.add(q_user, (internal_user_id, now, tx_id, from_app_name, str(internal_user_id), decimal_amount, currency, status_final, now, metadata_json))

        db.execute(batch)
        DEPOSIT_COUNT.inc() 

    except Exception as e:
        # CASO CRÍTICO: El dinero ya se acreditó (Paso 3), pero falló el log (Paso 4).
        # No podemos devolver error a la Central porque revertiría la operación y duplicaríamos saldo.
        # Solo logueamos el error crítico para revisión manual.
        logger.critical(f"¡DATA INCONSISTENCY! Dinero acreditado (Tx Central: {central_tx_id}) pero fallo en Cassandra: {e}", exc_info=True)
    
    # --- PASO 5: Confirmación ---
    # Respondemos el JSON que la API Central espera para marcar la tx como COMPLETED
    return {
        "success": True,
        "localTransactionId": str(tx_id)
    }

@app.post("/transfers/outbound-central", status_code=201, tags=["Central API"])
async def send_money_central(
    req: schemas.TransferRequest, 
    user_id: int = Header(..., alias="X-User-ID"),
    auth_token: str = Header(..., alias="Authorization"),
    db: Session = Depends(get_db)
):
    """
    Envía dinero a la API Centralizada (Versión Desplegada).
    Endpoint: /api/v1/sendTransfer
    """
    
    CENTRAL_API_URL = os.getenv("CENTRAL_API_URL")
    CENTRAL_WALLET_TOKEN = os.getenv("CENTRAL_WALLET_TOKEN")

    if not CENTRAL_API_URL or not CENTRAL_WALLET_TOKEN:
        logger.critical("Configuración Central incompleta.")
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Servicio no disponible")

    tx_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    currency = "PEN"
    
    # Metadatos iniciales
    metadata = {
        "to_app": req.to_bank, 
        "destination": req.destination_phone_number,
        "description": req.description
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        # --- PASO 1: Debitar Usuario Local (Balance Service) ---
        try:
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": user_id, "amount": req.amount}
            )
            debit_res.raise_for_status()
        except httpx.HTTPStatusError as e:
            # 400 = Fondos insuficientes
            detail = e.response.json().get("detail", "Error de débito") if e.response.content else "Error de débito"
            raise HTTPException(status_code=e.response.status_code, detail=detail)
        except Exception:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Error interno de saldo")

        # --- PASO 2: Llamar a API Central ---
        try:
            # 2a. Obtener teléfono del remitente
            auth_res = await client.get(f"{AUTH_SERVICE_URL}/users/{user_id}")
            auth_res.raise_for_status()
            sender_phone = auth_res.json().get("phone_number")

            if not sender_phone:
                 raise Exception("Usuario sin teléfono registrado")

            # 2b. Payload ajustado al Nuevo Contrato
            central_payload = {
                "fromIdentifier": sender_phone,
                "toIdentifier": req.destination_phone_number,
                "toAppName": req.to_bank, 
                "amount": req.amount,
                "externalTransactionId": str(tx_id),
                "description": req.description or "Transferencia Pixel Money"
            }
            logger.info(f"Enviando a Central: {central_payload}")

            # 2c. Headers
            central_headers = {
                "x-wallet-token": CENTRAL_WALLET_TOKEN, 
                "Authorization": auth_token,            
                "Content-Type": "application/json"
            }

            logger.info(f"Enviando a Central: {central_payload}")
            
            # 2d. POST a /sendTransfer (Nueva URL)
            central_res = await client.post(
                f"{CENTRAL_API_URL}/sendTransfer", 
                json=central_payload,
                headers=central_headers
            )

            # Validar éxito
            # Aceptamos 200 o 201 como éxito
            if central_res.status_code in [200, 201]:
                central_data = central_res.json()
                
                # Validar campo 'success' explícitamente
                if not central_data.get("success"):
                     raise Exception(f"API Central respondió 200 pero success=false: {central_data}")

                # Como 'data' viene vacía {}, no podemos guardar un ID externo.
                # Usamos nuestro propio ID como referencia.
                metadata["central_tx_id"] = "N/A (Ver externalTransactionId)"
                logger.info(f"Transferencia Central exitosa. Ref: {tx_id}")
            else:
                # Error HTTP (400, 404, 500)
                error_msg = central_res.text
                try:
                    error_msg = central_res.json().get("message", error_msg)
                except: pass
                raise Exception(f"API Central Error ({central_res.status_code}): {error_msg}")

        except Exception as e:
            logger.error(f"Fallo Central: {e}. Revirtiendo...")
            
            # --- REVERSIÓN (Compensación) ---
            try:
                await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit",
                    json={"user_id": user_id, "amount": req.amount}
                )
                logger.info(f"Reversión exitosa para user {user_id}")
            except Exception as rev_e:
                logger.critical(f"FALLO REVERSIÓN CRÍTICO: {rev_e}")
            
            # Mensaje limpio al usuario
            detail_msg = str(e) if "API Central Error" in str(e) else "Error de comunicación con el banco destino."
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=detail_msg)

    # --- PASO 3: Registrar en Cassandra ---
    try:
        decimal_amount = Decimal(str(req.amount))
        status_final = "COMPLETED"
        metadata_json = json.dumps(metadata)
        
        batch = BatchStatement()
        q_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDI', %s, 'CENTRAL_API', %s, 'TRANSFER_SENT', %s, %s, %s, %s, %s, %s)")
        q_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'BDI', %s, 'CENTRAL_API', %s, 'TRANSFER_SENT', %s, %s, %s, %s, %s)")

        batch.add(q_id, (tx_id, user_id, str(user_id), req.to_bank, decimal_amount, currency, status_final, now, now, metadata_json))
        batch.add(q_user, (user_id, now, tx_id, str(user_id), req.to_bank, decimal_amount, currency, status_final, now, metadata_json))

        db.execute(batch)
        TRANSFER_COUNT.inc()

    except Exception as e:
        logger.error(f"Error guardando historial outbound: {e}")

    return {
        "status": "COMPLETED", 
        "transaction_id": str(tx_id), 
        "message": "Transferencia enviada exitosamente"
    }

@app.post("/group-withdrawal", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal SAGA"])
async def execute_group_withdrawal(
    req: schemas.GroupWithdrawalRequest,
    db: Session = Depends(get_db)
    # NOTA: Esta es una ruta interna servicio-a-servicio.
    # No necesita 'idempotency_key' (el 'group_service' se encarga)
    # No necesita 'X-User-ID' (la lógica es interna)
):
    """
    EJECUTA la saga de retiro de grupo APROBADA POR EL LÍDER.
    1. Debita Balance del Grupo (BDG)
    2. Acredita Balance del Miembro (BDI)
    3. Actualiza el Saldo Interno del Miembro (Deuda) en GroupService
    4. Escribe 2 transacciones en Cassandra
    """
    logger.info(f"Ejecutando saga de retiro para request_id: {req.request_id} (Monto: {req.amount})")

    tx_id_debit = uuid.uuid4() # ID para la tx de SALIDA (del grupo)
    tx_id_credit = uuid.uuid4() # ID para la tx de ENTRADA (al miembro)
    now = datetime.now(timezone.utc)
    currency = "PEN"
    status_final = "FAILED_UNKNOWN" # Default

    # URLs de servicios (deben estar definidas al inicio del archivo)
    if not BALANCE_SERVICE_URL or not GROUP_SERVICE_URL:
         logger.error("URLs de servicio internas no configuradas en Ledger!")
         raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error de configuración interna.")

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:

            # --- PASO 1: Debitar Saldo del Grupo (BDG) ---
            # (¡Aquí usamos el endpoint que creamos en el Paso 166!)
            logger.debug(f"Tx {tx_id_debit}: Debitando {req.amount} de group_id {req.group_id}")
            debit_res = await client.post(
                f"{BALANCE_SERVICE_URL}/group_balance/debit",
                json={"group_id": req.group_id, "amount": req.amount}
            )
            debit_res.raise_for_status() # Falla aquí si el GRUPO no tiene fondos

            # --- PASO 2: Acreditar Saldo del Miembro (BDI) ---
            try:
                logger.debug(f"Tx {tx_id_credit}: Acreditando {req.amount} a user_id {req.member_user_id}")
                credit_res = await client.post(
                    f"{BALANCE_SERVICE_URL}/balance/credit",
                    json={"user_id": req.member_user_id, "amount": req.amount}
                )
                credit_res.raise_for_status()

            except Exception as credit_error:
                logger.error(f"¡FALLO DE SAGA (Retiro)! El crédito al miembro {req.member_user_id} falló. Revertiendo débito del grupo {tx_id_debit}...")
                # ¡REVERSIÓN! Devolvemos el dinero al grupo.
                async with httpx.AsyncClient() as revert_client:
                     await revert_client.post(f"{BALANCE_SERVICE_URL}/group_balance/credit", json={"group_id": req.group_id, "amount": req.amount})
                raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "El servicio de balance del miembro falló. La transacción ha sido revertida.")

            # --- PASO 3: Actualizar Saldo Interno (¡La Deuda!) ---
            try:
                logger.debug(f"Tx {tx_id_credit}: Actualizando internal_balance (DEUDA) para user {req.member_user_id}")
                internal_res = await client.post(
                    f"{GROUP_SERVICE_URL}/groups/{req.group_id}/member_balance",
                    json={
                        "user_id_to_update": req.member_user_id, 
                        "amount": -req.amount # ¡RESTAMOS el monto! (Genera la deuda)
                    }
                )
                internal_res.raise_for_status()

            except Exception as internal_error:
                # ¡FALLO CRÍTICO! El dinero se movió pero la deuda no se grabó.
                # (En un sistema V3.0, revertiríamos todo. Por ahora, solo logueamos.)
                logger.critical(f"¡FALLO CRÍTICO DE SAGA (Retiro)! El dinero se movió (Tx {tx_id_credit}) pero la deuda en group_service falló: {internal_error}")
                # No detenemos la transacción, el dinero ya se movió.

            # --- PASO 4: Todo OK ---
            status_final = "COMPLETED"

    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        detail = e.response.json().get("detail", "Error en servicios internos.")
        logger.warning(f"Saga de retiro fallida (Tx: {tx_id_debit}): {detail} (Status: {status_code})")
        # (Opcional: actualizar el 'withdrawal_request' a REJECTED)
        raise HTTPException(status_code=status_code, detail=detail)

    # ... (Otros except httpx.RequestError, Exception... se pueden añadir) ...

    # --- PASO 5: Escribir en Cassandra (BATCH) ---
    if status_final == "COMPLETED":
        try:
            decimal_amount = Decimal(str(req.amount))
            metadata = {"withdrawal_request_id": req.request_id}
            metadata_json = json.dumps(metadata)

            batch = BatchStatement()

            # Tx de SALIDA (para el historial del GRUPO)
            q_debit_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDG', %s, 'BDI', %s, 'GROUP_WITHDRAWAL', %s, %s, %s, %s, %s, %s)")
            q_debit_group = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_group (group_id, created_at, id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, %s, 'BDG', %s, 'BDI', %s, 'GROUP_WITHDRAWAL', %s, %s, %s, %s, %s)")
            batch.add(q_debit_id, (tx_id_debit, req.member_user_id, str(req.group_id), str(req.member_user_id), decimal_amount, currency, status_final, now, now, metadata_json))
            batch.add(q_debit_group, (req.group_id, now, tx_id_debit, req.member_user_id, str(req.group_id), str(req.member_user_id), decimal_amount, currency, status_final, now, metadata_json))

            # Tx de ENTRADA (para el historial del MIEMBRO)
            q_credit_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDG', %s, 'BDI', %s, 'DEPOSIT', %s, %s, %s, %s, %s, %s)")
            q_credit_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'BDG', %s, 'BDI', %s, 'DEPOSIT', %s, %s, %s, %s, %s)")
            batch.add(q_credit_id, (tx_id_credit, req.member_user_id, str(req.group_id), str(req.member_user_id), decimal_amount, currency, status_final, now, now, metadata_json))
            batch.add(q_credit_user, (req.member_user_id, now, tx_id_credit, str(req.group_id), str(req.member_user_id), decimal_amount, currency, status_final, now, metadata_json))

            db.execute(batch)

            # Devolvemos la transacción de ENTRADA (la que le importa al miembro)
            tx_data = await get_transaction_by_id(db, tx_id_credit)
            if not tx_data: raise Exception("No se pudo recuperar la transacción final")
            return schemas.Transaction(**tx_data)

        except Exception as e:
            logger.critical(f"¡FALLO CRÍTICO POST-SAGA! Tx {tx_id_debit} (Retiro) tuvo éxito pero Cassandra falló: {e}", exc_info=True)
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "El retiro se completó pero falló al registrarse.")

    # Si la saga falló antes de "COMPLETED" (ej. 400 Fondos Insuficientes)
    raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "La saga de retiro falló y no se completó.")



# --- SAGA DE PRÉSTAMOS (Loans) ---

@app.post("/loans/disbursement", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal SAGA"])
async def process_loan_disbursement(
    req: schemas.LoanEventRequest,
    db: Session = Depends(get_db)
):
    """
    SAGA: Desembolso de Préstamo via Balance Service.
    1. Ledger recibe orden.
    2. Ledger llama a Balance Service (/credit) para poner el dinero en la BDI.
    3. Ledger registra LOAN_DISBURSEMENT en Cassandra.
    """
    tx_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    currency = "PEN"
    status_final = "COMPLETED"
    
    metadata = {"loan_id": req.loan_id, "description": "Préstamo aprobado"}
    metadata_json = json.dumps(metadata)

    # 1. Mover el dinero (Llamar a Balance Service)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Acreditamos la cuenta del usuario (BDI)
            response = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/credit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            response.raise_for_status()
    except Exception as e:
        logger.error(f"Fallo al desembolsar préstamo en Balance Service: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al abonar el préstamo en la cuenta.")

    # 2. Registrar en Cassandra
    try:
        decimal_amount = Decimal(str(req.amount))
        batch = BatchStatement()

        # Tx ID Log (Historial General)
        q_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'PIXEL_BANK', 'MAIN_VAULT', 'BDI', %s, 'LOAN_DISBURSEMENT', %s, %s, %s, %s, %s, %s)")
        
        # Tx User Log (Historial Usuario)
        q_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'PIXEL_BANK', 'MAIN_VAULT', 'BDI', %s, 'LOAN_DISBURSEMENT', %s, %s, %s, %s, %s)")

        batch.add(q_id, (tx_id, req.user_id, str(req.user_id), decimal_amount, currency, status_final, now, now, metadata_json))
        batch.add(q_user, (req.user_id, now, tx_id, str(req.user_id), decimal_amount, currency, status_final, now, metadata_json))

        db.execute(batch)

        # Recuperar y devolver
        tx_data = await get_transaction_by_id(db, tx_id)
        return schemas.Transaction(**tx_data)

    except Exception as e:
        logger.critical(f"Dinero entregado pero fallo en Cassandra (Loan Disbursement): {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Préstamo entregado pero error al registrar historial.")


@app.post("/loans/payment", response_model=schemas.Transaction, status_code=status.HTTP_201_CREATED, tags=["Internal SAGA"])
async def process_loan_payment(
    req: schemas.LoanEventRequest,
    db: Session = Depends(get_db)
):
    """
    SAGA: Pago de Préstamo via Balance Service.
    1. Ledger recibe orden.
    2. Ledger llama a Balance Service (/debit) para cobrar.
    3. Ledger registra LOAN_PAYMENT en Cassandra.
    """
    tx_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    currency = "PEN"
    status_final = "COMPLETED"
    
    metadata = {"loan_id": req.loan_id, "description": "Pago de préstamo"}
    metadata_json = json.dumps(metadata)

    # 1. Cobrar el dinero (Llamar a Balance Service)
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            # Debitamos la cuenta del usuario (BDI)
            response = await client.post(
                f"{BALANCE_SERVICE_URL}/balance/debit",
                json={"user_id": req.user_id, "amount": req.amount}
            )
            response.raise_for_status() # Esto lanzará error 400 si no hay fondos
    except httpx.HTTPStatusError as e:
         raise HTTPException(status_code=e.response.status_code, detail=f"Fallo el cobro: {e.response.json().get('detail')}")
    except Exception as e:
        logger.error(f"Fallo al cobrar préstamo en Balance Service: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error al procesar el cobro del préstamo.")

    # 2. Registrar en Cassandra
    try:
        decimal_amount = Decimal(str(req.amount))
        batch = BatchStatement()

        # Tx ID Log
        q_id = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions (id, user_id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, created_at, updated_at, metadata) VALUES (%s, %s, 'BDI', %s, 'PIXEL_BANK', 'MAIN_VAULT', 'LOAN_PAYMENT', %s, %s, %s, %s, %s, %s)")
        
        # Tx User Log
        q_user = SimpleStatement(f"INSERT INTO {KEYSPACE}.transactions_by_user (user_id, created_at, id, source_wallet_type, source_wallet_id, destination_wallet_type, destination_wallet_id, type, amount, currency, status, updated_at, metadata) VALUES (%s, %s, %s, 'BDI', %s, 'PIXEL_BANK', 'MAIN_VAULT', 'LOAN_PAYMENT', %s, %s, %s, %s, %s)")

        batch.add(q_id, (tx_id, req.user_id, str(req.user_id), decimal_amount, currency, status_final, now, now, metadata_json))
        batch.add(q_user, (req.user_id, now, tx_id, str(req.user_id), decimal_amount, currency, status_final, now, metadata_json))

        db.execute(batch)

        tx_data = await get_transaction_by_id(db, tx_id)
        return schemas.Transaction(**tx_data)

    except Exception as e:
        logger.critical(f"Dinero cobrado pero fallo en Cassandra (Loan Payment): {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Préstamo cobrado pero error al registrar historial.")





# --- Endpoint de Salud y Métricas ---

@app.get("/health", tags=["Monitoring"])
def health_check():
    """Verifica la salud básica del servicio y la conexión a Cassandra."""
    db_status = "ok"
    try:
        if db_session:
            
            db_session.execute("SELECT now() FROM system.local", timeout=3.0) 
        else:
            db_status = "error - session not initialized"
            raise HTTPException(status_code=503, detail="Sesión de BD no inicializada")
    except Exception as e:
        logger.error(f"Health check fallido - Error de Cassandra: {e}", exc_info=True)
        db_status = "error"
        # Devolvemos 503 para que el healthcheck de Docker falle
        raise HTTPException(status_code=503, detail=f"Database (Cassandra) connection error: {e}")

    return {"status": "ok", "service": "ledger_service", "database": db_status}

@app.get("/metrics", tags=["Monitoring"])
def metrics():
    """Expone métricas de la aplicación para Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)