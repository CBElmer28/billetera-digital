import logging
import time
import os
import httpx 
import uuid 
from fastapi import FastAPI, Depends, HTTPException, status, Request, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer
from fastapi.responses import Response
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy.orm import Session
from typing import Optional, List
from utils import get_name_from_reniec

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
    CENTRAL_API_URL,
    CENTRAL_WALLET_TOKEN,
    APP_NAME,
    register_user_in_central,
    create_password_reset_token, 
    verify_reset_token, 
    send_reset_email, 
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
    Registra usuario usando DNI. El nombre se obtiene automáticamente de RENIEC.
    """
    logger.info(f"Registro iniciado para DNI: {user.dni}")

    # 1. Validaciones de Unicidad
    if db.query(User).filter(User.email == user.email).first():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Email ya registrado.")
    if db.query(User).filter(User.phone_number == user.phone_number).first():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Celular ya registrado.")
    if db.query(User).filter(User.dni == user.dni).first():
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "DNI ya registrado.")

    # 2. Obtener Nombre Automático
    real_name = await get_name_from_reniec(user.dni)
    logger.info(f"RENIEC devolvió: {real_name}")

    # 3. Crear Usuario
    hashed_password = get_password_hash(user.password)
    new_user = User(
        dni=user.dni,
        name=real_name,
        email=user.email,
        phone_number=user.phone_number,
        hashed_password=hashed_password
    )

    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
    except Exception as e:
        db.rollback()
        logger.error(f"DB Error: {e}")
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error guardando usuario.")

    # 4. Crear Cuenta en Balance (Igual que antes)
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

    # 4. Llamar a API Central (SAGA Paso 2 - Doble Registro)
    # Generamos un token temporal válido para la Central (con la SECRET_KEY compartida)
    temp_token = create_access_token(data={"sub": str(new_user.id), "name": new_user.name})
    
    # Usamos la función de utilidad que encapsula la complejidad
    await register_user_in_central(
        user_id=new_user.id,
        phone_number=new_user.phone_number,
        user_name=new_user.name,
        auth_token=temp_token
    )

    return new_user
    


@app.post("/login", response_model=schemas.Token, tags=["Authentication"])
def login(db: Session = Depends(get_db), form_data: OAuth2PasswordRequestForm = Depends()):
    """
    Autentica al usuario y genera una Single Active Session.
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

    # 1. Generar un nuevo Session ID único
    new_session_id = str(uuid.uuid4())

    # 2. Guardarlo en la base de datos (Esto invalida cualquier sesión anterior)
    user.session_id = new_session_id
    db.commit() # Guardamos cambios
    db.refresh(user)

    # 3. Incluir el session_id en el token JWT
    token_data = {
        "sub": str(user.id),
        "name": user.name,
        "session_id": new_session_id 
    }

    access_token = create_access_token(data=token_data)
    logger.info(f"Login successful for user_id: {user.id}. Session ID: {new_session_id}")

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
    Usado internamente por el API Gateway y Ledger.
    """
    logger.info(f"Solicitud de datos para usuario con ID {user_id}")

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        logger.warning(f"Usuario con ID {user_id} no encontrado.")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado.",
        )

    # --- CORRECCIÓN AQUÍ ---
    # Antes devolvíamos un dict manual {id, name...} y faltaba el DNI.
    # Ahora devolvemos el objeto 'user' completo. 
    # Gracias a `from_attributes=True` en el schema, Pydantic extraerá el DNI solito.
    return user

@app.get("/verify", response_model=schemas.TokenPayload, tags=["Internal"])
def verify(token: str, db: Session = Depends(get_db)): 
    """
    Valida el token y asegura que corresponda a la sesión activa actual.
    """
    # 1. Decodificar token (Valida firma y expiración)
    payload_dict = decode_token(token)
    
    if payload_dict is None or "sub" not in payload_dict:
        logger.warning("Token inválido, expirado o sin 'sub'.")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )

    user_id = payload_dict.get("sub")
    token_session_id = payload_dict.get("session_id")

    # 2. Verificar contra la base de datos
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Usuario no existe")

    # 3. COMPARACIÓN CRÍTICA: ¿Es el session_id del token igual al de la BD?
    if user.session_id != token_session_id:
        logger.warning(f"Intento de uso de sesión invalidada para user {user_id}. Token: {token_session_id} vs DB: {user.session_id}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sesión expirada o iniciada en otro dispositivo"
        )

    return {
        "sub": str(user_id), 
        "exp": payload_dict.get("exp"), 
        "name": payload_dict.get("name"),
        "session_id": token_session_id
    }

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
    

@app.post("/users/{user_id}/verify-password", tags=["Internal"])
def verify_password_endpoint(
    user_id: int,
    check: schemas.PasswordCheck,
    db: Session = Depends(get_db)
):
    """
    Valida si la contraseña enviada coincide con la del usuario.
    Uso interno para confirmar transacciones sensibles.
    """
    # 1. Buscar usuario
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")
    
    # 2. Verificar contraseña usando la utilidad existente
    if not verify_password(check.password, user.hashed_password):
        # Retornamos 401 si está mal
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Contraseña incorrecta")
    
    # 3. Si todo ok
    return {"valid": True}



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

@app.post("/request-password-reset", status_code=200)
async def request_password_reset(
    request: schemas.PasswordResetRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    # 1. Buscar usuario
    user = db.query(User).filter(User.email == request.email).first()
    
    # 2. Si el usuario existe, procesar. 
    # IMPORTANTE: Por seguridad, siempre respondemos "Si el correo existe, se envió..." 
    # para no revelar qué correos están registrados.
    if user:
        # Generar token
        token = create_password_reset_token(user.email)
        # Enviar email en segundo plano
        background_tasks.add_task(send_reset_email, user.email, token)
    
    return {"message": "Si el correo está registrado, recibirás instrucciones en breve."}

# Endpoint 2: Confirmar cambio
@app.post("/reset-password", status_code=200)
async def reset_password(
    request: schemas.PasswordResetConfirm,
    db: Session = Depends(get_db)
):
    # 1. Validar que las contraseñas coincidan
    if request.new_password != request.confirm_password:
        raise HTTPException(status_code=400, detail="Las contraseñas no coinciden")

    # 2. Validar token
    email = verify_reset_token(request.token)
    if not email:
        raise HTTPException(status_code=400, detail="Token inválido o expirado")

    # 3. Buscar usuario
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    # 4. Actualizar contraseña
    user.hashed_password = get_password_hash(request.new_password)
    db.commit()

    return {"message": "Contraseña actualizada correctamente"}