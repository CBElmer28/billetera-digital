import logging
import time
import httpx
import os # <-- Necesario para leer variables de entorno
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy.orm import Session
from typing import Optional, List

# Importaciones locales
from db import engine, Base, get_db
from models import User
import db
import schemas
import models
from utils import (
    get_password_hash,
    verify_password,
    create_access_token,
    decode_token,
    BALANCE_SERVICE_URL,
    # --- IMPORTACIONES DE SEGURIDAD (Del Bloque 1) ---
    generate_verification_code,
    send_telegram_message,
    VERIFICATION_CODE_EXPIRATION_MINUTES
)

# --- CONFIGURACIÓN MODO ESTRÉS ---
# True = Se salta Telegram (comportamiento Bloque 2). 
# False = Pide Telegram (comportamiento Bloque 1).
STRESS_TEST_MODE = os.getenv("STRESS_TEST_MODE", "False").lower() == "true"

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Crea tablas si no existen al iniciar
try:
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified/created.")
except Exception as e:
    logger.error(f"Error initializing database: {e}", exc_info=True)

# Inicializa FastAPI
app = FastAPI(
    title="Auth Service - Pixel Money",
    description="Handles user registration, authentication, and token verification.",
    version="1.1.0" # Actualizado por la fusión
)

if STRESS_TEST_MODE:
    logger.warning("⚠️ MODO STRESS ACTIVADO: Verificación por Telegram deshabilitada ⚠️")

# --- Métricas Prometheus ---
REQUEST_COUNT = Counter("auth_requests_total", "Total requests processed", ["method", "endpoint", "status_code"])
REQUEST_LATENCY = Histogram("auth_request_latency_seconds", "Request latency in seconds", ["endpoint"])

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
        logger.error(f"Unhandled exception: {exc}", exc_info=True)
        return Response("Internal Server Error", status_code=500)
    finally:
        latency = time.time() - start_time
        endpoint = request.url.path
        final_status_code = getattr(response, 'status_code', status_code)
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
        REQUEST_COUNT.labels(method=request.method, endpoint=endpoint, status_code=final_status_code).inc()
    return response

# --- Endpoints de Salud y Métricas ---
@app.get("/metrics", tags=["Monitoring"])
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/health", tags=["Monitoring"])
def health_check():
    return {"status": "ok", "service": "auth_service", "stress_mode": STRESS_TEST_MODE}

# --- Endpoints de API ---

@app.post("/register", response_model=schemas.UserResponse, status_code=status.HTTP_201_CREATED, tags=["Authentication"])
async def register(user: schemas.UserCreate, db: Session = Depends(get_db)):
    """
    FUSIÓN INTELIGENTE:
    - Si STRESS_TEST_MODE es True: Registra directo (estilo Bloque 2).
    - Si STRESS_TEST_MODE es False: Usa Telegram (estilo Bloque 1).
    """
    logger.info(f"Registration attempt for email: {user.email}")
    
    # 1. Validaciones Comunes
    if db.query(User).filter(User.email == user.email).first():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Email already registered")
    if db.query(User).filter(User.phone_number == user.phone_number).first():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Número de celular ya registrado")
    if db.query(User).filter(User.telegram_chat_id == user.telegram_chat_id).first():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "ID de Chat de Telegram ya registrado")

    hashed_password = get_password_hash(user.password)

    # === RAMIFICACIÓN DE LÓGICA ===
    
    if STRESS_TEST_MODE:
        # --- LÓGICA RÁPIDA (Simula Bloque 2) ---
        new_user = User(
            name=user.name,         
            email=user.email,
            hashed_password=hashed_password,
            phone_number=user.phone_number,
            telegram_chat_id=user.telegram_chat_id,
            is_phone_verified=True, # Directamente verificado
            phone_verification_code=None,
            phone_verification_expires=None
        )
        # Guardar y llamar a Balance
        try:
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
        except Exception as e:
            db.rollback()
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Could not save user.")

        # Llamada Sincrónica a Balance (Típica de stress tests)
        async with httpx.AsyncClient() as client:
            try:
                create_account_url = f"{BALANCE_SERVICE_URL}/accounts"
                response = await client.post(create_account_url, json={"user_id": new_user.id})
                response.raise_for_status()
            except Exception as exc:
                # En modo estrés, si falla balance, borramos user
                db.delete(new_user)
                db.commit()
                raise HTTPException(status_code=503, detail="Balance Service failed (Stress Mode).")
        
        return new_user

    else:
        # --- LÓGICA SEGURA (Bloque 1 - Develop) ---
        verification_code = generate_verification_code()
        expires_at = datetime.utcnow() + timedelta(minutes=VERIFICATION_CODE_EXPIRATION_MINUTES)

        new_user = User(
            name=user.name,         
            email=user.email,
            hashed_password=hashed_password,
            phone_number=user.phone_number,
            telegram_chat_id=user.telegram_chat_id,
            is_phone_verified=False, # <--- Importante: Falso al inicio
            phone_verification_code=verification_code,
            phone_verification_expires=expires_at
        )

        try:
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            logger.info(f"User created unverified ID: {new_user.id}")
        except Exception as e:
            db.rollback()
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Could not save user.")

        # Enviar Telegram
        try:
            message = f"Hola *{new_user.name}*, bienvenido a Pixel Money.\nTu código de verificación es: `{verification_code}`\nEste código expira en {VERIFICATION_CODE_EXPIRATION_MINUTES} minutos."
            await send_telegram_message(new_user.telegram_chat_id, message)
        except Exception as e:
            logger.error(f"Fallo envío Telegram: {e}")
            # No revertimos, el usuario puede pedir reenvío

        return new_user


@app.post("/login", response_model=schemas.Token, tags=["Authentication"])
def login(db: Session = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    user = db.query(User).filter(User.email == form_data.username).first()
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Incorrect email or password", headers={"WWW-Authenticate": "Bearer"})

    token_data = {"sub": str(user.id), "name": user.name}
    access_token = create_access_token(data=token_data)

    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "user_id": user.id,
        "name": user.name,
        "email": user.email,
        "is_phone_verified": user.is_phone_verified
    }

# --- ENDPOINTS DE VERIFICACIÓN (Del Bloque 1 - Develop) ---

@app.post("/verify-phone", response_model=schemas.UserResponse, tags=["Authentication"])
async def verify_phone(verification_data: schemas.PhoneVerificationRequest, db: Session = Depends(get_db)):
    logger.info(f"Verificando {verification_data.phone_number}")
    user = db.query(User).filter(User.phone_number == verification_data.phone_number).first()
    
    if not user: raise HTTPException(404, "Usuario no encontrado")
    if user.is_phone_verified: raise HTTPException(400, "Ya verificado")
    if user.phone_verification_code != verification_data.code: raise HTTPException(400, "Código incorrecto")
    if not user.phone_verification_expires or user.phone_verification_expires < datetime.utcnow():
        raise HTTPException(400, "Código expirado")

    # Crear cuenta en Balance Service
    if not BALANCE_SERVICE_URL: raise HTTPException(503, "Error config interna")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(f"{BALANCE_SERVICE_URL}/accounts", json={"user_id": user.id})
            response.raise_for_status()
        except Exception as exc:
            logger.error(f"Fallo Balance Service: {exc}")
            raise HTTPException(503, "Error conectando con servicio de balance")

    user.is_phone_verified = True
    user.phone_verification_code = None
    user.phone_verification_expires = None
    db.commit()
    db.refresh(user)
    return user

@app.post("/resend-code", status_code=status.HTTP_204_NO_CONTENT, tags=["Authentication"])
async def resend_verification_code(request_data: schemas.RequestVerificationCode, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.phone_number == request_data.phone_number).first()
    if not user: raise HTTPException(404, "Usuario no encontrado")
    if user.is_phone_verified: raise HTTPException(400, "Ya verificado")

    verification_code = generate_verification_code()
    user.phone_verification_code = verification_code
    user.phone_verification_expires = datetime.utcnow() + timedelta(minutes=VERIFICATION_CODE_EXPIRATION_MINUTES)
    db.commit()

    try:
        message = f"Tu nuevo código es: `{verification_code}`"
        await send_telegram_message(user.telegram_chat_id, message)
    except:
        raise HTTPException(503, "Error enviando mensaje")
    
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.get("/users/{user_id}", response_model=schemas.UserResponse, tags=["Users"])
async def get_user_by_id(user_id: int, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user: raise HTTPException(404, "Usuario no encontrado")
    return user

@app.get("/verify", response_model=schemas.TokenPayload, tags=["Internal"])
def verify(token: str): 
    payload = decode_token(token)
    if not payload or "sub" not in payload: raise HTTPException(401, "Token inválido")
    return {"sub": payload.get("sub"), "exp": payload.get("exp"), "name": payload.get("name")}

@app.get("/users/by-phone/{phone_number}", response_model=schemas.UserResponse, tags=["Users"])
def get_user_by_phone(phone_number: str, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.phone_number == phone_number).first()
    if not user: raise HTTPException(404, "Usuario no encontrado")
    return user

@app.post("/users/bulk", response_model=List[schemas.UserResponse], tags=["Users"])
def get_users_bulk(req: schemas.UserBulkRequest, db: Session = Depends(get_db)):
    return db.query(User).filter(User.id.in_(req.user_ids)).all()

@app.post("/users/{user_id}/change-password", tags=["Users"])
def change_password(user_id: int, req: schemas.PasswordChangeRequest, token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    # Lógica de cambio de contraseña (Del bloque 1/2 fusionados)
    payload = decode_token(token)
    if not payload or int(payload.get("sub")) != user_id: raise HTTPException(403, "No autorizado")

    user = db.query(User).filter(User.id == user_id).first()
    if not user: raise HTTPException(404, "Usuario no encontrado")

    if not verify_password(req.current_password, user.hashed_password): raise HTTPException(400, "Password actual incorrecto")
    if req.new_password != req.confirm_password: raise HTTPException(400, "Confirmación no coincide")
    
    user.hashed_password = get_password_hash(req.new_password)
    db.commit()
    return {"message": "Contraseña actualizada"}

# --- ENDPOINT NUEVO (Del Bloque 2 - Feature Stress Test) ---

@app.delete("/users/{user_id}", tags=["Users"])
async def delete_user(user_id: int, db: Session = Depends(get_db)):
    """
    Elimina el usuario. Coordina con Balance Service (Feature traída del Bloque 2).
    """
    logger.info(f"Eliminando usuario {user_id}")
    user = db.query(User).filter(User.id == user_id).first()
    if not user: raise HTTPException(404, "Usuario no encontrado")

    if not BALANCE_SERVICE_URL: raise HTTPException(500, "Error configuración Balance URL")

    # Llamada a Balance Service para borrar datos
    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(f"{BALANCE_SERVICE_URL}/accounts/{user_id}")
            if response.status_code == 400:
                raise HTTPException(400, response.json().get('detail', 'Deuda pendiente'))
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400: raise HTTPException(400, e.response.json().get('detail'))
            raise HTTPException(503, "Error contactando Balance Service")

    db.delete(user)
    db.commit()
    return {"message": "Cuenta eliminada"}