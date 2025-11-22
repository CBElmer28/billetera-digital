"""Pruebas automatizadas para verificar la consulta de saldo individual (/balance/me)."""

import requests
import pytest # Importar pytest para mensajes de error más claros

# Importar la URL base y fixtures desde conftest
from conftest import GATEWAY_URL

def test_get_initial_balance(auth_headers):
    """
    Verifica que el endpoint /balance/me devuelve correctamente el saldo inicial (0.0 USD)
    para un usuario recién registrado.
    Utiliza el token obtenido de la fixture 'auth_headers'.
    """
    balance_url = f"{GATEWAY_URL}/balance/me"
    user_id_for_logging = "(obtenido del token)" # No tenemos el ID aquí, solo el token

    print(f"\n[Test] Obteniendo saldo inicial para usuario {user_id_for_logging}...")
    try:
        r = requests.get(balance_url, headers=auth_headers, timeout=10)
        r.raise_for_status() # Lanza excepción si el código de estado no es 2xx
        account_data = r.json()
        print(f"[Test] Respuesta de saldo recibida: {account_data}")

        # --- Verificaciones Esenciales ---
        assert isinstance(account_data, dict), "La respuesta debe ser un diccionario JSON."
        assert "user_id" in account_data, "La respuesta debe contener 'user_id'."
        assert "balance" in account_data, "La respuesta debe contener 'balance'."
        assert "currency" in account_data, "La respuesta debe contener 'currency'."

        # Verifica los valores iniciales esperados
        assert account_data["balance"] == 0.0, \
            f"Saldo inicial incorrecto. Esperado 0.0, recibido {account_data['balance']}."
        assert account_data["currency"] == "USD", \
            f"Moneda inicial incorrecta. Esperado 'USD', recibido '{account_data['currency']}'."

        print(f"[Test] Saldo inicial (0.0 USD) verificado correctamente.")

    except requests.exceptions.Timeout:
         pytest.fail(f"Fallo en la prueba: Timeout al llamar a {balance_url}.")
    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else "Sin respuesta"
        pytest.fail(f"Fallo en la prueba: Error al llamar a {balance_url}. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}\nRespuesta: {error_text}")
    except AssertionError as e:
        pytest.fail(f"Fallo en la prueba: La verificación falló - {e}")
    except Exception as e:
        pytest.fail(f"Fallo inesperado en la prueba: {e}")