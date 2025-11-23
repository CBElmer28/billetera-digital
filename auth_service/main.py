import logging
import time
import httpx
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
)

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
    version="1.0.0"
)

# --- Métricas Prometheus ---
REQUEST_COUNT = Counter(
    "auth_requests_total",
    "Total requests processed by Auth Service",
    ["method", "endpoint", "status_code"]
)
REQUEST_LATENCY = Histogram(
    "auth_request_latency_seconds",
    "Request latency in seconds for Auth Service",
    ["endpoint"]
)

# --- Middleware para Métricas ---
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = None
    status_code = 500 # Default a 500

    try:
        response = await call_next(request)
        status_code = response.status_code
    except HTTPException as http_exc:
        status_code = http_exc.status_code
        raise http_exc
    except Exception as exc:
        logger.error(f"Unhandled exception during request processing: {exc}", exc_info=True)
        
        return Response("Internal Server Error", status_code=500)
    finally:
        latency = time.time() - start_time
        endpoint = request.url.path


        # Obtener status_code final de forma segura
        final_status_code = getattr(response, 'status_code', status_code)

        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=final_status_code
        ).inc()

    return response

# --- Endpoints de Salud y Métricas ---
@app.get("/metrics", tags=["Monitoring"])
def metrics():
    """Exposes application metrics for Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/health", tags=["Monitoring"])
def health_check():
    """Performs a basic health check of the service."""
    
    return {"status": "ok", "service": "auth_service"}

# --- Endpoints de API ---

@app.post("/register", response_model=schemas.UserResponse, status_code=status.HTTP_201_CREATED, tags=["Authentication"])
async def register(user: schemas.UserCreate, db: Session = Depends(get_db)):
    """
    Registers a new user with email and password.
    Creates the user in the database and calls the Balance Service to create an associated account.
    """
    logger.info(f"Registration attempt for email: {user.email}")
    db_user = db.query(User).filter(User.email == user.email).first()
    if db_user:
        logger.warning(f"Registration failed: Email {user.email} already exists.")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )
    
        
    db_phone = db.query(User).filter(User.phone_number == user.phone_number).first()
    if db_phone:
        raise HTTPException(status_code=400, detail="Número de celular ya registrado")
    

    hashed_password = get_password_hash(user.password)

    new_user = User(
        name=user.name,        
        email=user.email,
        hashed_password=hashed_password,
        phone_number=user.phone_number
    )

    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        logger.info(f"User created with ID: {new_user.id} for email: {user.email}")
    except Exception as e:
        db.rollback()
        logger.error(f"Database error during user creation for email {user.email}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Could not save user.")

    
    async with httpx.AsyncClient() as client:
        try:
            create_account_url = f"{BALANCE_SERVICE_URL}/accounts"
            response = await client.post(create_account_url, json={"user_id": new_user.id})
            response.raise_for_status() 
            logger.info(f"Successfully called Balance Service for user_id: {new_user.id}")
        except (httpx.RequestError, httpx.HTTPStatusError) as exc:
            logger.error(f"Failed to call Balance Service for user_id {new_user.id}: {exc}", exc_info=True)
            
            logger.warning(f"Attempting to revert user creation for user_id {new_user.id} due to Balance Service failure.")
            try:
                db.delete(new_user)
                db.commit()
                logger.info(f"Successfully reverted user creation for user_id {new_user.id}.")
            except Exception as delete_e:
                
                logger.critical(f"CRITICAL: Failed to revert user creation for user_id {new_user.id}: {delete_e}", exc_info=True)
               

            status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            detail = f"Balance Service unavailable or failed."
            if isinstance(exc, httpx.HTTPStatusError):
                status_code = exc.response.status_code
                try: 
                    detail = f"Balance Service error: {exc.response.json().get('detail', exc.response.text)}"
                except: 
                     detail = f"Balance Service error: Status {status_code}"
            raise HTTPException(status_code=status_code, detail=detail)

    return {
        "id": new_user.id,
        "name": new_user.name,
        "email": new_user.email,
        "phone_number": new_user.phone_number
    }


@app.post("/login", response_model=schemas.Token, tags=["Authentication"])
def login(db: Session = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Authenticates a user based on email (as username) and password (form-data).
    Returns a JWT access token upon successful authentication.
    """
    logger.info(f"Login attempt for user: {form_data.username}")
    user = db.query(User).filter(User.email == form_data.username).first()

    if not user or not verify_password(form_data.password, user.hashed_password):
        logger.warning(f"Login failed for user: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

  
        # Preparamos el payload del token (¡EL ESTÁNDAR!)
    # El 'sub' (subject) es el ID del usuario.
    token_data = {
        "sub": str(user.id),
        "name": user.name # Añadimos el nombre para el frontend
    }

    access_token = create_access_token(data=token_data)
    logger.info(f"Login successful for user_id: {user.id}")

    # ¡Devolvemos el objeto COMPLETO que el schema espera!
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "user_id": user.id,
        "name": user.name,
        "email": user.email
    }

@app.get("/users/{user_id}", response_model=schemas.UserResponse, tags=["Users"])
async def get_user_by_id(user_id: int, db: Session = Depends(get_db)):
    """
    Retorna la información del usuario por su ID.
    Usado internamente por el API Gateway al llamar /auth/me.
    """
    logger.info(f"Solicitud de datos para usuario con ID {user_id}")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        logger.warning(f"Usuario con ID {user_id} no encontrado.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado.",
        )

    logger.info(f"Usuario encontrado: {user.email} (ID: {user.id})")

    return {
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "phone_number": user.phone_number
    }


@app.get("/verify", response_model=schemas.TokenPayload, tags=["Internal"])
def verify(token: str): 
    """
    Valida un token JWT (pasado como query parameter 'token') y devuelve su payload.
    Usado por el API Gateway.
    """
    payload = decode_token(token)
    if payload is None or "sub" not in payload: # Doble verificación
     logger.warning("Intento de verificación con token inválido, expirado o sin 'sub'.")
     raise HTTPException(
         status_code=status.HTTP_401_UNAUTHORIZED,
         detail="Invalid or expired token",
     )

    # Devolvemos el payload que SÍ coincide con schemas.TokenPayload
    # (El gateway_service ahora leerá 'sub' sin problemas)
    return {"sub": payload.get("sub"), "exp": payload.get("exp"), "name": payload.get("name")}

@app.post("/users/{user_id}/change-password", tags=["Users"])
def change_user_password(
    user_id: int, 
    req: schemas.PasswordChangeRequest, 
    db: Session = Depends(get_db)
):
    """Cambia la contraseña del usuario validando la actual."""
    logger.info(f"Intento de cambio de contraseña para user_id: {user_id}")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")

    # 1. Verificar que la contraseña actual sea correcta
    if not verify_password(req.current_password, user.hashed_password):
        logger.warning(f"Fallo cambio de password user {user_id}: Password actual incorrecto")
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "La contraseña actual es incorrecta.")

    # 2. Encriptar la nueva contraseña
    user.hashed_password = get_password_hash(req.new_password)
    
    try:
        db.commit()
        logger.info(f"Contraseña actualizada exitosamente para user {user_id}")
        return {"message": "Contraseña actualizada correctamente"}
    except Exception as e:
        db.rollback()
        logger.error(f"Error DB al cambiar password: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno al actualizar contraseña")
    

# En auth_service/main.py (Junto a los otros endpoints de usuario)

@app.delete("/users/{user_id}", tags=["Users"])
async def delete_user(user_id: int, db: Session = Depends(get_db)):
    """
    Elimina el usuario del sistema.
    Coordina con Balance Service para verificar deudas primero.
    """
    logger.info(f"Iniciando proceso de eliminación para usuario {user_id}")
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")

    # 1. Llamar a Balance Service para verificar y borrar datos financieros
    if not BALANCE_SERVICE_URL:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Configuración interna incompleta (Balance URL)")

    async with httpx.AsyncClient() as client:
        try:
            response = await client.delete(f"{BALANCE_SERVICE_URL}/accounts/{user_id}")
            
            # Si Balance Service dice que hay deuda (400), detenemos todo
            if response.status_code == 400:
                detail = response.json().get('detail', 'Deuda pendiente')
                raise HTTPException(status.HTTP_400_BAD_REQUEST, detail)
                
            response.raise_for_status() # Para otros errores (500, etc)
            
        except httpx.HTTPStatusError as e:
            # Re-lanzamos el error específico si viene del microservicio
            if e.response.status_code == 400:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, e.response.json().get('detail'))
            logger.error(f"Error contactando Balance Service: {e}")
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "No se pudo verificar el estado financiero.")

    # 2. Si llegamos aquí, no hay deuda. Procedemos a borrar el usuario.
    try:
        db.delete(user)
        db.commit()
        logger.info(f"Usuario {user_id} eliminado permanentemente.")
        return {"message": "Cuenta eliminada exitosamente"}
    except Exception as e:
        db.rollback()
        logger.error(f"Error DB borrando usuario: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno al eliminar usuario")



@app.get("/users/by-phone/{phone_number}", response_model=schemas.UserResponse, tags=["Users"])
def get_user_by_phone(phone_number: str, db: Session = Depends(get_db)):
    """
    Busca un usuario por su número de celular.
    (Usado internamente por ledger_service para transferencias P2P).
    """
    logger.info(f"Buscando usuario por número de celular: {phone_number}")
    db_user = db.query(User).filter(User.phone_number == phone_number).first()
    if db_user is None:
        raise HTTPException(status_code=404, detail="Usuario no encontrado con ese número de celular")
    return db_user

# ... (después de 'get_user_by_phone')

@app.post("/users/bulk", response_model=List[schemas.UserResponse], tags=["Users"])
def get_users_bulk(req: schemas.UserBulkRequest, db: Session = Depends(get_db)):
    """
    Obtiene los detalles públicos de una lista de IDs de usuario.
    Usado por group_service para enriquecer la lista de miembros.
    """
    logger.info(f"Solicitud de datos para {len(req.user_ids)} usuarios.")

    # Usamos 'in_' para buscar múltiples IDs a la vez en la BD
    users = db.query(User).filter(User.id.in_(req.user_ids)).all()

    return users