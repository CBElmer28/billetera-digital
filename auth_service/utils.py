"""Funciones de utilidad para el servicio de autenticación, incluyendo hash de contraseñas y manejo de JWT."""

import os
import logging
import bcrypt
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
from jose import JWTError, jwt
from dotenv import load_dotenv
from typing import Dict, Optional
from db import get_db

# Carga variables de entorno desde .env
load_dotenv()

# Configuración del logger
logger = logging.getLogger(__name__)

# --- Configuración de Seguridad ---
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not SECRET_KEY:
    logger.warning("JWT_SECRET_KEY no está definida en las variables de entorno. Usando clave insegura por defecto para desarrollo.")
    
    SECRET_KEY = "clave_secreta_insegura_por_defecto_cambiar_urgentemente" 

ALGORITHM = "HS256"

ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60 * 24))


pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    
)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verifica una contraseña plana contra un hash almacenado."""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Genera el hash de una contraseña plana usando bcrypt."""
    return pwd_context.hash(password)

# --- Utilidades para Tokens JWT ---
def create_access_token(data: Dict) -> str:
    """
    Genera un token de acceso JWT con los datos proporcionados y una marca de tiempo de expiración.

    Args:
        data: Diccionario (payload) a incluir en el token (ej., {'sub': user_id}).

    Returns:
        String del JWT codificado.
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_token(token: str) -> Optional[Dict]:
    """
    Decodifica y valida un token JWT.

    Args:
        token: El string JWT a decodificar.

    Returns:
        El diccionario del payload decodificado si el token es válido y no ha expirado,
        en caso contrario, None.
    """
    try:
        payload = jwt.decode(
            token,
            SECRET_KEY,
            algorithms=[ALGORITHM],
            options={"verify_aud": False} 
        )
       
        if payload.get("exp") and datetime.now(timezone.utc) < datetime.fromtimestamp(payload["exp"], tz=timezone.utc):
             return payload
        else:
            logger.warning("Fallo en decodificación de token: El token ha expirado.")
            return None
    except JWTError as e:
        logger.warning(f"Fallo en decodificación de token: {e}")
        return None
    except Exception as e: # Captura cualquier otro error inesperado
        logger.error(f"Error inesperado durante decodificación de token: {e}", exc_info=True)
        return None

# --- Service Discovery ---
# URL interna para el Balance Service
BALANCE_SERVICE_URL = os.getenv("BALANCE_SERVICE_URL")
if not BALANCE_SERVICE_URL:
     logger.error("Variable de entorno BALANCE_SERVICE_URL no está definida.")