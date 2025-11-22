import logging
import time
import os
import httpx
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, status, Request, Header
from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from dotenv import load_dotenv

# Importaciones locales
from db import engine, Base, get_db, SessionLocal
from models import Account, GroupAccount, Loan, LoanStatus
import schemas
import models

# Carga variables de entorno
load_dotenv()

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE ENTORNO (Del .env que proporcionaste) ---
LEDGER_SERVICE_URL = os.getenv("LEDGER_SERVICE_URL")
DECOLECTA_API_URL = os.getenv("DECOLECTA_API_URL")
DECOLECTA_TOKEN = os.getenv("DECOLECTA_TOKEN")

# Crea tablas si no existen
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Tablas de base de datos verificadas/creadas.")
except Exception as e:
    logger.error(f"Error al inicializar la base de datos: {e}", exc_info=True)

app = FastAPI(
    title="Balance Service - Pixel Money",
    description="Gestiona saldos, préstamos y orquestación con Ledger.",
    version="2.1.0"
)

# --- MÉTRICAS PROMETHEUS ---
REQUEST_COUNT = Counter("balance_requests_total", "Total requests", ["method", "endpoint", "status_code"])
REQUEST_LATENCY = Histogram("balance_request_latency_seconds", "Request latency", ["endpoint"])

# Métricas de Negocio (Stress-test feature)
BANK_PROFIT_GAUGE = Gauge('bank_profit_total', 'Ganancia total acumulada del banco')
BANK_LOANS_GAUGE = Gauge('bank_loans_total', 'Cantidad total de préstamos')
BANK_LENT_GAUGE = Gauge('bank_lent_total', 'Monto total prestado')

def update_metrics_from_db(db: Session):
    """Recalcula las métricas de negocio leyendo la base de datos."""
    try:
        paid_loans = db.query(Loan).filter(Loan.status == LoanStatus.PAID).all()
        total_profit = sum(l.principal_amount * (l.interest_rate / 100) for l in paid_loans)
        
        all_loans = db.query(Loan).all()
        total_lent = sum(l.principal_amount for l in all_loans)
        
        BANK_PROFIT_GAUGE.set(float(total_profit))
        BANK_LENT_GAUGE.set(float(total_lent))
        BANK_LOANS_GAUGE.set(len(all_loans))
    except Exception as e:
        logger.error(f"Error actualizando métricas: {e}")

@app.on_event("startup")
def startup_event():
    try:
        db = SessionLocal()
        update_metrics_from_db(db)
        db.close()
    except Exception:
        pass

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
        # Normalización simple de endpoints
        if "/balance/" in endpoint and endpoint.split('/')[-1].isdigit(): endpoint = "/balance/{user_id}"
        
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
        final_code = getattr(response, 'status_code', status_code)
        REQUEST_COUNT.labels(method=request.method, endpoint=endpoint, status_code=final_code).inc()
    return response

@app.get("/metrics", tags=["Monitoring"])
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/health", tags=["Monitoring"])
def health_check():
    return {"status": "ok", "service": "balance_service", "ledger_connected": bool(LEDGER_SERVICE_URL)}

# --- HELPER: Validación DNI (Stress-test feature) ---
async def validar_dni_reniec(dni: str) -> str:
    if dni == "99999999": return "Usuario Test (Stress)"
    if not dni or len(dni) != 8 or not dni.isdigit():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "DNI inválido.")
    
    if not DECOLECTA_API_URL or not DECOLECTA_TOKEN:
        return "Usuario Validado (Sin API Externa)"

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{DECOLECTA_API_URL}?numero={dni}",
                headers={"Authorization": f"Bearer {DECOLECTA_TOKEN}"},
                timeout=5.0
            )
            if response.status_code == 200:
                return response.json().get("full_name") or "Ciudadano"
            elif response.status_code == 404:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "DNI no encontrado en RENIEC.")
    except Exception:
        return "Validación Pendiente (Error API)"
    return "Ciudadano Peruano"

# --- ENDPOINTS BDI (Cuentas Individuales) ---

@app.post("/accounts", response_model=schemas.AccountResponse, status_code=status.HTTP_201_CREATED, tags=["BDI Accounts"])
def create_account(account_in: schemas.AccountCreate, db: Session = Depends(get_db)):
    new_account = Account(user_id=account_in.user_id, balance=0.0)
    try:
        db.add(new_account)
        db.commit()
        db.refresh(new_account)
        return new_account
    except IntegrityError:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, detail="Account already exists.")

@app.get("/balance/{user_id}", response_model=schemas.AccountResponse, tags=["BDI Balance"])
def get_balance(user_id: int, db: Session = Depends(get_db)):
    # Usamos joinedload (Develop logic) para traer info del préstamo si existe
    account = db.query(Account).options(joinedload(Account.loan)).filter(Account.user_id == user_id).first()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Account not found.")
    return account

@app.post("/balance/check", tags=["BDI Balance"])
def check_funds(check_in: schemas.BalanceCheck, db: Session = Depends(get_db)):
    # Corrección Decimal (Develop logic)
    amount_check = Decimal(str(check_in.amount))
    account = db.query(Account).filter(Account.user_id == check_in.user_id).first()
    if not account:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Account not found.")
    if account.balance < amount_check:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Insufficient funds.")
    return {"message": "Sufficient funds"}

@app.post("/balance/credit", response_model=schemas.AccountResponse, tags=["BDI Balance"])
def credit_balance(update_in: schemas.BalanceUpdate, db: Session = Depends(get_db)):
    """
    Acredita fondos. Usa BLOQUEO PESIMISTA (Develop logic) para seguridad.
    """
    try:
        with db.begin():
            account = db.query(Account).filter(Account.user_id == update_in.user_id).with_for_update().first()
            if not account:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found.")
            
            # Conversión segura Decimal
            account.balance += Decimal(str(update_in.amount))
            db.commit()
        db.refresh(account)
        return account
    except Exception as e:
        db.rollback()
        logger.error(f"Error credit: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal error.")

@app.post("/balance/debit", response_model=schemas.AccountResponse, tags=["BDI Balance"])
def debit_balance(update_in: schemas.BalanceUpdate, db: Session = Depends(get_db)):
    """
    Debita fondos. Usa BLOQUEO PESIMISTA + VERIFICACIÓN DECIMAL (Develop logic).
    """
    amount_to_debit = Decimal(str(update_in.amount))
    try:
        with db.begin():
            account = db.query(Account).filter(Account.user_id == update_in.user_id).with_for_update().first()
            if not account:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "Account not found.")
            
            if account.balance < amount_to_debit:
                db.rollback() # Liberar bloqueo
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Insufficient funds.")
            
            account.balance -= amount_to_debit
            db.commit()
        db.refresh(account)
        return account
    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback()
        logger.error(f"Error debit: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Internal error.")

# --- ENDPOINTS BDI (Préstamos con SAGA) ---

@app.post("/request-loan", response_model=schemas.AccountResponse, tags=["BDI Préstamos"])
async def request_loan(
    req: schemas.DepositRequest,
    x_user_id: int = Header(..., alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    """
    Solicita préstamo. Usa validación RENIEC y orquestación con Ledger (Stress-test logic).
    """
    user_id = x_user_id
    amount_principal = Decimal(str(req.amount))
    
    # 1. Validación Externa (RENIEC)
    nombre_real = await validar_dni_reniec(req.dni)
    logger.info(f"Préstamo user {user_id} - DNI {req.dni} ({nombre_real})")

    MAX_LOAN = Decimal('500.00')
    if amount_principal > MAX_LOAN:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Monto excede límite.")

    INTEREST_RATE = Decimal('0.05')
    total_debt = amount_principal * (1 + INTEREST_RATE)

    try:
        with db.begin():
            # Check deuda previa
            existing = db.query(Loan).filter(Loan.user_id == user_id, Loan.status == LoanStatus.ACTIVE).first()
            if existing:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, "Ya tienes un préstamo activo.")

            # Crear préstamo local
            new_loan = Loan(
                user_id=user_id,
                dni=req.dni,
                principal_amount=amount_principal,
                outstanding_balance=total_debt,
                interest_rate=INTEREST_RATE * 100,
                status=LoanStatus.ACTIVE
            )
            db.add(new_loan)
            db.commit()
        
        db.refresh(new_loan)

        # 2. SAGA: Llamar al Ledger (Microservicio)
        # Esto es vital dado que tienes LEDGER_SERVICE_URL en tu .env
        if LEDGER_SERVICE_URL:
            async with httpx.AsyncClient() as client:
                try:
                    res = await client.post(
                        f"{LEDGER_SERVICE_URL}/loans/disbursement",
                        json={
                            "user_id": user_id,
                            "amount": float(amount_principal),
                            "loan_id": new_loan.id
                        }
                    )
                    res.raise_for_status()
                except Exception as e:
                    # Compensación: Si falla Ledger, borramos el préstamo local
                    logger.error(f"Fallo Ledger SAGA: {e}")
                    db.delete(new_loan)
                    db.commit()
                    raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Error conectando con Ledger.")
        else:
            # Fallback por si el env está mal configurado, acreditamos local
            update_in = schemas.BalanceUpdate(user_id=user_id, amount=float(amount_principal))
            credit_balance(update_in, db)

        update_metrics_from_db(db)
        return db.query(Account).filter(Account.user_id == user_id).first()

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error critico prestamo: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno.")

@app.post("/pay-loan", response_model=schemas.LoanResponse, tags=["BDI Préstamos"])
async def pay_loan(
    x_user_id: int = Header(..., alias="X-User-ID"),
    db: Session = Depends(get_db)
):
    user_id = x_user_id
    
    loan = db.query(Loan).filter(Loan.user_id == user_id, Loan.status == LoanStatus.ACTIVE).first()
    if not loan:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "No tienes préstamos activos.")

    amount_to_pay = loan.outstanding_balance

    try:
        # SAGA: Cobro a través del Ledger
        if LEDGER_SERVICE_URL:
            async with httpx.AsyncClient() as client:
                res = await client.post(
                    f"{LEDGER_SERVICE_URL}/loans/payment",
                    json={
                        "user_id": user_id,
                        "amount": float(amount_to_pay),
                        "loan_id": loan.id
                    }
                )
                res.raise_for_status()
        else:
             # Fallback local
             update_in = schemas.BalanceUpdate(user_id=user_id, amount=float(amount_to_pay))
             debit_balance(update_in, db)

        # Cerrar préstamo localmente
        loan.outstanding_balance = Decimal('0.00')
        loan.status = LoanStatus.PAID
        db.commit()
        db.refresh(loan)
        update_metrics_from_db(db)
        return loan

    except httpx.HTTPStatusError as e:
        # Propagar error del Ledger (ej: fondos insuficientes)
        detail = "Error pago."
        try: detail = e.response.json().get('detail')
        except: pass
        raise HTTPException(e.response.status_code, detail)
    except Exception as e:
        logger.error(f"Error pago: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno.")

# --- ENDPOINTS GRUPALES (BDG) - IGUAL QUE DEVELOP ---
@app.post("/group_accounts", response_model=schemas.GroupAccount, status_code=status.HTTP_201_CREATED, tags=["Balance - Grupal"])
def create_group_account(account_in: schemas.GroupAccountCreate, db: Session = Depends(get_db)):
    # Lógica estándar...
    try:
        new_account = GroupAccount(group_id=account_in.group_id, balance=0.00)
        db.add(new_account)
        db.commit()
        db.refresh(new_account)
        return new_account
    except IntegrityError:
        db.rollback()
        raise HTTPException(status.HTTP_409_CONFLICT, "Group account exists.")

@app.get("/group_balance/{group_id}", response_model=schemas.GroupAccount, tags=["Balance - Grupal"])
def get_group_balance(group_id: int, db: Session = Depends(get_db)):
    account = db.query(GroupAccount).filter(GroupAccount.group_id == group_id).first()
    if not account: raise HTTPException(404, "Not found")
    return account

@app.post("/group_balance/credit", response_model=schemas.GroupAccount, tags=["Balance - Grupal"])
def credit_group_balance(update_in: schemas.GroupBalanceUpdate, db: Session = Depends(get_db)):
    try:
        with db.begin():
            account = db.query(GroupAccount).filter(GroupAccount.group_id == update_in.group_id).with_for_update().first()
            if not account: raise HTTPException(404, "Not found")
            account.balance += Decimal(str(update_in.amount))
            db.commit()
        db.refresh(account)
        return account
    except Exception as e:
        db.rollback()
        raise e

@app.post("/group_balance/debit", response_model=schemas.GroupAccount, tags=["Balance - Grupal"])
def debit_group_balance(update_in: schemas.GroupBalanceUpdate, db: Session = Depends(get_db)):
    amount = Decimal(str(update_in.amount))
    try:
        with db.begin():
            account = db.query(GroupAccount).filter(GroupAccount.group_id == update_in.group_id).with_for_update().first()
            if not account: raise HTTPException(404, "Not found")
            if account.balance < amount: raise HTTPException(400, "Insufficient funds")
            account.balance -= amount
            db.commit()
        db.refresh(account)
        return account
    except Exception as e:
        db.rollback()
        raise e

# --- ENDPOINT INTERNO (Stress-test feature) ---
@app.delete("/accounts/{user_id}", tags=["Internal"])
def delete_account_internal(user_id: int, db: Session = Depends(get_db)):
    """Permite al Auth Service eliminar usuarios (Si no tienen deuda)."""
    active_loan = db.query(Loan).filter(Loan.user_id == user_id, Loan.status == LoanStatus.ACTIVE).first()
    if active_loan:
        raise HTTPException(400, f"Deuda pendiente: {active_loan.outstanding_balance}")

    try:
        with db.begin():
            db.query(Loan).filter(Loan.user_id == user_id).delete()
            db.query(Account).filter(Account.user_id == user_id).delete()
            db.commit()
        return {"message": "Eliminado"}
    except Exception as e:
        db.rollback()
        raise HTTPException(500, "Error eliminando.")