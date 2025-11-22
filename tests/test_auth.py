"""Pruebas automatizadas para los endpoints de autenticación (/auth/*)."""

import requests
import uuid
import pytest # Importar pytest para mensajes de error más claros

# Importar la URL base desde conftest
from conftest import GATEWAY_URL

def test_register_duplicate_email(test_user_token):
    """
    Verifica que el endpoint /auth/register devuelve un error (400 o 409)
    cuando se intenta registrar un email que ya existe.
    Utiliza el email del usuario creado en la fixture 'test_user_token'.
    """
    existing_email = test_user_token['email']
    register_payload = {"email": existing_email, "password": "some_other_password"}
    register_url = f"{GATEWAY_URL}/auth/register"

    print(f"\n[Test] Intentando registrar email duplicado: {existing_email}...")
    r = requests.post(register_url, json=register_payload, timeout=10)

    # El servicio auth_service debería devolver 400 (Bad Request) o 409 (Conflict)
    expected_status_codes = [400, 409]
    assert r.status_code in expected_status_codes, \
        f"Registro duplicado fallido. Esperado status {expected_status_codes}, recibido {r.status_code}. Respuesta: {r.text}"
    print(f"[Test] Registro duplicado correctamente rechazado con status {r.status_code}.")

def test_login_invalid_credentials():
    """
    Verifica que el endpoint /auth/login devuelve un error 401 (Unauthorized)
    cuando se proporcionan credenciales incorrectas (email no existente o contraseña errónea).
    """
    # Usamos un email aleatorio que garantizamos no existe
    non_existent_email = f"nouser_{uuid.uuid4()}@example.com"
    login_payload = {"username": non_existent_email, "password": "wrong_password"} # username=email para form-data
    login_url = f"{GATEWAY_URL}/auth/login"

    print(f"\n[Test] Intentando login con credenciales inválidas: {non_existent_email}...")
    r = requests.post(login_url, data=login_payload, timeout=10)

    # Esperamos un error 401 (Unauthorized)
    assert r.status_code == 401, \
        f"Login inválido fallido. Esperado status 401, recibido {r.status_code}. Respuesta: {r.text}"
    print(f"[Test] Login inválido correctamente rechazado con status 401.")