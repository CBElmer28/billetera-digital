"""Funciones de utilidad para el servicio de autenticación, incluyendo hash de contraseñas y manejo de JWT."""

import os
import logging
import bcrypt
import random 
import string 
import httpx 
from datetime import datetime, timedelta, timezone
from passlib.context import CryptContext
from jose import JWTError, jwt
from dotenv import load_dotenv
from typing import Dict, Optional
from db import get_db
from fastapi import HTTPException

# Carga variables de entorno desde .env
load_dotenv()

# Configuración del logger
logger = logging.getLogger(__name__)

# --- Configuración de Seguridad ---
SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not SECRET_KEY:
    logger.warning("JWT_SECRET_KEY no está definida. Usando clave insegura por defecto.")
    SECRET_KEY = "clave_secreta_insegura_por_defecto_cambiar_urgentemente" 

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", 60 * 24))

# --- Configuración de Verificación y Telegram (Vital para el modo Híbrido) ---
VERIFICATION_CODE_EXPIRATION_MINUTES = int(os.getenv("VERIFICATION_CODE_EXPIRATION_MINUTES", 10))
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Advertencia solo si no estamos en modo test, pero para simplificar lo dejamos logueado
if not TELEGRAM_BOT_TOKEN:
    logger.warning("TELEGRAM_BOT_TOKEN no definido. El envío de mensajes fallará si no estás en STRESS_TEST_MODE.")

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
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def decode_token(token: str) -> Optional[Dict]:
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
            logger.warning("Token expirado.")
            return None
    except JWTError as e:
        logger.warning(f"Error decodificando token: {e}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado token: {e}", exc_info=True)
        return None

# --- Service Discovery ---
BALANCE_SERVICE_URL = os.getenv("BALANCE_SERVICE_URL")
if not BALANCE_SERVICE_URL:
     logger.error("Variable BALANCE_SERVICE_URL no definida.")
     
# --- FUNCIONES DE VERIFICACIÓN (Requeridas por Develop y Main Híbrido) ---

def generate_verification_code(length: int = 6) -> str:
    """Genera un código numérico aleatorio de 6 dígitos."""
    return "".join(random.choices(string.digits, k=length))

async def send_telegram_message(chat_id: str, message: str) -> bool:
    """
    Envía un mensaje a un chat_id de Telegram.
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.error("No se puede enviar mensaje: Token no configurado.")
        return False

    url = f"{TELEGRAM_API_URL}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown"
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            return True
        except Exception as exc:
            logger.error(f"Error enviando Telegram a {chat_id}: {exc}")
            return False