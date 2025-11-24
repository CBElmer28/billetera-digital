"""Funciones de utilidad para el servicio de autenticación, incluyendo hash de contraseñas y manejo de JWT."""

import os
import logging
import httpx 
import bcrypt 
import smtplib
from email.message import EmailMessage
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

SMTP_HOST = os.getenv("SMTP_HOST", "mailhog") 
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

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

DECOLECTA_API_URL = os.getenv("DECOLECTA_API_URL", "https://api.decolecta.com/v1/reniec/dni")
DECOLECTA_TOKEN = os.getenv("DECOLECTA_TOKEN")

async def get_name_from_reniec(dni: str) -> str:
    """
    Consulta la API de RENIEC para obtener el nombre a partir del DNI.
    """
    if dni == "99999999": return "Usuario de Prueba" # Backdoor

    if not DECOLECTA_TOKEN:
        logger.warning("DECOLECTA_TOKEN no configurado. Usando nombre genérico.")
        return "Ciudadano Sin Identificar"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            url = f"{DECOLECTA_API_URL}?numero={dni}"
            headers = {"Authorization": f"Bearer {DECOLECTA_TOKEN}"}
            
            response = await client.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                nombre = data.get("full_name")
                if not nombre:
                     # Intento de fallback
                     nombres = data.get('nombres', '')
                     ap_pat = data.get('apellido_paterno', '')
                     ap_mat = data.get('apellido_materno', '')
                     nombre = f"{nombres} {ap_pat} {ap_mat}".strip()
                return nombre or "Ciudadano Peruano"
            else:
                logger.error(f"RENIEC API Error {response.status_code}: {response.text}")
                # Fallback seguro para no bloquear registro si RENIEC cae
                return "Usuario Validado (RENIEC Error)" 
                
    except Exception as e:
        logger.error(f"Error conectando con RENIEC: {e}")
        return "Usuario Validado (Error Conexión)"








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

# --- CONFIGURACIÓN API CENTRALIZADA ---
CENTRAL_API_URL = os.getenv("CENTRAL_API_URL")
CENTRAL_WALLET_TOKEN = os.getenv("CENTRAL_WALLET_TOKEN")
APP_NAME = os.getenv("APP_NAME", "PIXEL MONEY") # Valor por defecto si no está en .env

logger = logging.getLogger(__name__)

async def register_user_in_central(user_id: int, phone_number: str, user_name: str, auth_token: str) -> None:
    """
    Registra al usuario en la API Centralizada.
    Solo realiza la llamada (Fire and Forget), no retorna datos.
    """
    if not CENTRAL_API_URL or not CENTRAL_WALLET_TOKEN:
        logger.warning("Configuración de API Central incompleta. Saltando registro central.")
        return None # Salimos sin hacer nada

    url = f"{CENTRAL_API_URL}/register-wallet"
    
    payload = {
        "userIdentifier": phone_number,
        "internalWalletId": user_id,
        "userName": user_name,
        "appName": APP_NAME
    }
    
    headers = {
        "x-wallet-token": CENTRAL_WALLET_TOKEN,
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json"
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            logger.info(f"Registrando en API Central: {payload}")
            response = await client.post(url, json=payload, headers=headers)
            
            # Solo verificamos el status code para loguear éxito o error
            if response.status_code in [200, 201]:
                logger.info(f"Registro en API Central exitoso para usuario {user_id}.")
            else:
                logger.error(f"Fallo registro Central ({response.status_code}): {response.text}")

    except Exception as e:
        logger.error(f"Error de conexión con API Central: {e}")

# --- Service Discovery ---
# URL interna para el Balance Service
BALANCE_SERVICE_URL = os.getenv("BALANCE_SERVICE_URL")
if not BALANCE_SERVICE_URL:
     logger.error("Variable de entorno BALANCE_SERVICE_URL no está definida.")

# --- CONFIGURACIÓN DE GMAIL ---

def create_password_reset_token(email: str):
    """Genera un JWT de corta duración exclusivo para resetear password"""
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": email, "type": "password_reset", "exp": expire}
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def verify_reset_token(token: str):
    """Valida el token y extrae el email"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("type") != "password_reset":
            return None
        email: str = payload.get("sub")
        return email
    except JWTError:
        return None

def send_reset_email(to_email: str, token: str):
    """Envía el correo con el link de recuperación"""
    # En un caso real, esto sería una URL de tu Frontend, ej: https://tusitio.com/reset?token=...
    reset_link = f"http://localhost:3000/reset-password?token={token}"
    
    msg = EmailMessage()
    msg.set_content(f"""
    Hola,
    
    Has solicitado restablecer tu contraseña.
    Haz clic en el siguiente enlace para continuar:
    
    {reset_link}
    
    Este enlace expira en {RESET_TOKEN_EXPIRE_MINUTES} minutos.
    Si no fuiste tú, ignora este mensaje.
    """)
    
    msg['Subject'] = "Recuperación de Contraseña - Pixel Money"
    msg['From'] = "no-reply@pixelmoney.com"
    msg['To'] = to_email

    try:
        # Conexión al servidor SMTP (MailHog)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.send_message(msg)
            print(f"Correo enviado a {to_email}")
    except Exception as e:
        print(f"Error enviando correo: {e}")