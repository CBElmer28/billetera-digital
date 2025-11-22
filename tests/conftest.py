"""
Configuraciones y fixtures compartidos para las pruebas automatizadas con pytest.
Define la URL base del Gateway y crea un usuario de prueba con token al inicio de la sesión.
"""

import pytest
import requests
import uuid
from jose import jwt # Necesario para decodificar el token (usa python-jose)
import os # Para leer JWT_SECRET_KEY del entorno (o default)
from dotenv import load_dotenv

# Cargar variables de entorno (para JWT_SECRET_KEY)
# Asume que hay un .env en la raíz del proyecto o que la variable está en el entorno
load_dotenv(dotenv_path='./auth_service/.env')

# URL base del API Gateway
GATEWAY_URL = "http://localhost:8080"
# Secreto para decodificar el token (debe coincidir con auth_service/utils.py y .env)
JWT_SECRET = os.getenv("JWT_SECRET_KEY", "default_insecure_secret_key_change_this_immediately")
ALGORITHM = "HS256"

@pytest.fixture(scope="session")
def test_user_token():
    """
    Fixture de sesión: Se ejecuta una vez al inicio de todas las pruebas.
    1. Registra un usuario único para la sesión de pruebas.
    2. Inicia sesión con ese usuario para obtener un token JWT.
    3. Decodifica el token para obtener el user_id.
    4. Devuelve un diccionario con email, user_id y token.
    """
    session_uuid = uuid.uuid4()
    test_email = f"testuser_{session_uuid}@example.com"
    test_password = "password123"
    user_id = None
    token = None

    # --- 1. Registro ---
    register_payload = {"email": test_email, "password": test_password}
    register_url = f"{GATEWAY_URL}/auth/register"
    try:
        print(f"\n[Fixture] Registrando usuario de prueba: {test_email}...")
        r_register = requests.post(register_url, json=register_payload, timeout=10)
        # Permitir 201 (creado) o 400/409 (si ya existe por alguna razón)
        if r_register.status_code not in [201, 400, 409]:
             r_register.raise_for_status() # Lanza excepción para otros errores
        user_data = r_register.json()
        # Intentamos obtener el ID por si la respuesta lo incluye (depende de auth_service)
        # user_id = user_data.get("id")
        print(f"[Fixture] Registro OK (o usuario ya existía).")
    except requests.exceptions.RequestException as e:
        pytest.fail(f"Fallo CRÍTICO en fixture: No se pudo registrar usuario de prueba en {register_url}. Error: {e}\nRespuesta: {e.response.text if e.response else 'N/A'}")
    except Exception as e:
         pytest.fail(f"Fallo CRÍTICO en fixture durante registro: {e}")


    # --- 2. Login ---
    login_payload = {"username": test_email, "password": test_password} # username = email para login form
    login_url = f"{GATEWAY_URL}/auth/login"
    try:
        print(f"[Fixture] Iniciando sesión como: {test_email}...")
        r_login = requests.post(login_url, data=login_payload, timeout=10)
        r_login.raise_for_status() # Falla si login no es 200 OK
        token_data = r_login.json()
        token = token_data.get("access_token")
        if not token:
             pytest.fail(f"Fallo CRÍTICO en fixture: Login exitoso pero no se recibió access_token.")
        print(f"[Fixture] Login exitoso.")
    except requests.exceptions.RequestException as e:
        pytest.fail(f"Fallo CRÍTICO en fixture: No se pudo iniciar sesión en {login_url}. Error: {e}\nRespuesta: {e.response.text if e.response else 'N/A'}")
    except Exception as e:
         pytest.fail(f"Fallo CRÍTICO en fixture durante login: {e}")

    # --- 3. Decodificar Token para obtener User ID ---
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM], options={"verify_aud": False})
        user_id_str = payload.get("sub")
        if not user_id_str:
             pytest.fail(f"Fallo CRÍTICO en fixture: Token decodificado no contiene 'sub' (user_id). Payload: {payload}")
        user_id = int(user_id_str)
        print(f"[Fixture] Token decodificado, user_id obtenido: {user_id}")
    except (jwt.JWTError, ValueError, TypeError) as e:
        pytest.fail(f"Fallo CRÍTICO en fixture: No se pudo decodificar el token o extraer user_id. Error: {e}")

    return {"email": test_email, "user_id": user_id, "token": token}


@pytest.fixture(scope="session")
def auth_headers(test_user_token):
    """Fixture simple para obtener las cabeceras de autorización Bearer."""
    return {"Authorization": f"Bearer {test_user_token['token']}"}


@pytest.fixture
def idempotency_key() -> str:
    """Fixture para generar una clave de idempotencia UUID única para cada prueba."""
    return str(uuid.uuid4())