"""Pruebas automatizadas para los endpoints del Ledger Service (/ledger/*)."""

import requests
import pytest
import uuid
import logging # Añadido para logging en pruebas

# Importar la URL base y fixtures desde conftest
from conftest import GATEWAY_URL

# Configurar un logger simple para las pruebas
logger = logging.getLogger(__name__)

# --- Función Auxiliar ---
def get_current_balance(headers: dict) -> float:
    """Obtiene el saldo actual del usuario autenticado llamando al endpoint /balance/me."""
    balance_url = f"{GATEWAY_URL}/balance/me"
    try:
        r = requests.get(balance_url, headers=headers, timeout=10)
        r.raise_for_status()
        balance = r.json()["balance"]
        logger.info(f"Saldo actual obtenido: {balance}")
        return float(balance) # Aseguramos que sea float
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        pytest.fail(f"Fallo al obtener saldo actual en {balance_url}. Error: {e}")
        return 0.0 # Necesario para que el linter no se queje, aunque pytest.fail detiene

# --- Pruebas ---

def test_deposit_updates_balance(auth_headers, idempotency_key):
    """
    Verifica el flujo de depósito BDI:
    1. Obtiene el saldo inicial.
    2. Realiza un depósito usando una clave de idempotencia.
    3. Verifica que la transacción se completó.
    4. Obtiene el saldo final y verifica que aumentó correctamente.
    """
    deposit_url = f"{GATEWAY_URL}/ledger/deposit"
    deposit_amount = 150.75
    headers = {**auth_headers, "Idempotency-Key": idempotency_key}
    payload = {"amount": deposit_amount} # El Gateway inyectará el user_id

    print(f"\n[Test] Depósito: Verificando actualización de saldo...")
    try:
        initial_balance = get_current_balance(auth_headers)
        print(f"[Test] Depósito: Saldo inicial = {initial_balance}")

        print(f"[Test] Depósito: Realizando depósito de {deposit_amount} con key {idempotency_key}...")
        r_deposit = requests.post(deposit_url, json=payload, headers=headers, timeout=15)
        r_deposit.raise_for_status()
        deposit_tx = r_deposit.json()
        print(f"[Test] Depósito: Respuesta recibida -> {deposit_tx}")

        assert deposit_tx.get("status") == "COMPLETED", \
            f"Estado de transacción incorrecto. Esperado 'COMPLETED', recibido '{deposit_tx.get('status')}'"
        assert deposit_tx.get("amount") == deposit_amount, \
            f"Monto de transacción incorrecto. Esperado {deposit_amount}, recibido {deposit_tx.get('amount')}"

        # --- Verificación Crítica del Saldo ---
        final_balance = get_current_balance(auth_headers)
        print(f"[Test] Depósito: Saldo final = {final_balance}")
        expected_balance = initial_balance + deposit_amount
        # Usamos pytest.approx para comparar floats con una pequeña tolerancia
        assert final_balance == pytest.approx(expected_balance), \
               f"Saldo incorrecto después del depósito. Esperado ~{expected_balance}, recibido {final_balance}"

        print(f"[Test] Depósito: Saldo actualizado correctamente.")

    except requests.exceptions.Timeout:
         pytest.fail(f"Fallo en prueba de depósito: Timeout al llamar a {deposit_url}.")
    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else "Sin respuesta"
        pytest.fail(f"Fallo en prueba de depósito: Error en {deposit_url}. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}\nRespuesta: {error_text}")
    except (AssertionError, KeyError) as e:
        pytest.fail(f"Fallo en prueba de depósito: Verificación fallida - {e}")
    except Exception as e:
        pytest.fail(f"Fallo inesperado en prueba de depósito: {e}")


def test_deposit_idempotency(auth_headers, idempotency_key):
    """
    Verifica la idempotencia del depósito:
    1. Obtiene saldo inicial.
    2. Realiza un depósito con una clave.
    3. Realiza el MISMO depósito con la MISMA clave.
    4. Verifica que el ID de transacción devuelto sea el mismo.
    5. Verifica que el saldo solo haya aumentado UNA VEZ.
    """
    deposit_url = f"{GATEWAY_URL}/ledger/deposit"
    deposit_amount = 50.0
    headers = {**auth_headers, "Idempotency-Key": idempotency_key} # Usar la MISMA clave
    payload = {"amount": deposit_amount}

    print(f"\n[Test] Idempotencia Depósito: Verificando depósito único...")
    try:
        initial_balance = get_current_balance(auth_headers)
        print(f"[Test] Idempotencia Depósito: Saldo inicial = {initial_balance}")

        # --- Primer Depósito ---
        print(f"[Test] Idempotencia Depósito: Realizando primer depósito de {deposit_amount} con key {idempotency_key}...")
        r1 = requests.post(deposit_url, json=payload, headers=headers, timeout=15)
        r1.raise_for_status()
        tx1 = r1.json()
        tx1_id = tx1.get("id")
        assert tx1_id, "El primer depósito no devolvió un ID de transacción."
        print(f"[Test] Idempotencia Depósito: Primer depósito OK (ID: {tx1_id}).")

        # --- Segundo Depósito (Duplicado) ---
        print(f"[Test] Idempotencia Depósito: Realizando segundo depósito (duplicado) con la misma key...")
        r2 = requests.post(deposit_url, json=payload, headers=headers, timeout=15)
        r2.raise_for_status() # Esperamos que devuelva 2xx (la transacción original)
        tx2 = r2.json()
        tx2_id = tx2.get("id")
        assert tx2_id, "El segundo depósito (duplicado) no devolvió un ID de transacción."
        print(f"[Test] Idempotencia Depósito: Segundo depósito OK (ID: {tx2_id}).")

        # --- Verificaciones ---
        assert tx1_id == tx2_id, \
            f"Idempotencia fallida: IDs de transacción diferentes. Original: {tx1_id}, Duplicado: {tx2_id}"

        final_balance = get_current_balance(auth_headers)
        print(f"[Test] Idempotencia Depósito: Saldo final = {final_balance}")
        expected_balance = initial_balance + deposit_amount # Solo debe aumentar una vez

        assert final_balance == pytest.approx(expected_balance), \
               f"Idempotencia fallida: Saldo incorrecto. Esperado ~{expected_balance}, recibido {final_balance}"

        print(f"[Test] Idempotencia Depósito: Verificada correctamente.")

    except requests.exceptions.Timeout:
         pytest.fail(f"Fallo en prueba de idempotencia: Timeout al llamar a {deposit_url}.")
    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else "Sin respuesta"
        pytest.fail(f"Fallo en prueba de idempotencia: Error en {deposit_url}. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}\nRespuesta: {error_text}")
    except (AssertionError, KeyError) as e:
        pytest.fail(f"Fallo en prueba de idempotencia: Verificación fallida - {e}")
    except Exception as e:
        pytest.fail(f"Fallo inesperado en prueba de idempotencia: {e}")


def test_transfer_bdi_to_bdi_updates_balance(auth_headers, idempotency_key):
    """
    Verifica el flujo de transferencia BDI -> BDI (a Happy Money simulado):
    1. Asegura fondos depositando primero.
    2. Obtiene saldo inicial.
    3. Realiza la transferencia usando API interbancaria (número de celular).
    4. Verifica que la transacción se completó.
    5. Obtiene saldo final y verifica que disminuyó correctamente.
    """
    transfer_url = f"{GATEWAY_URL}/ledger/transfer"
    transfer_amount = 120.25
    destination_phone = "987654321" # Número de prueba válido para el simulador

    headers = {**auth_headers, "Idempotency-Key": idempotency_key}
    payload = {
        "amount": transfer_amount,
        "to_bank": "HAPPY_MONEY", # Banco destino correcto
        "destination_phone_number": destination_phone # Campo correcto
    } # El Gateway inyectará el user_id

    print(f"\n[Test] Transferencia BDI->BDI: Verificando actualización de saldo...")
    try:
        # 1. Asegurar fondos suficientes
        print("[Test] Transferencia BDI->BDI: Depositando fondos iniciales...")
        deposit_key = str(uuid.uuid4())
        deposit_headers = {**auth_headers, "Idempotency-Key": deposit_key}
        # Depositamos suficiente para cubrir la transferencia y posibles pruebas anteriores
        requests.post(f"{GATEWAY_URL}/ledger/deposit", json={"amount": transfer_amount + 200.0}, headers=deposit_headers, timeout=15).raise_for_status()

        initial_balance = get_current_balance(auth_headers)
        print(f"[Test] Transferencia BDI->BDI: Saldo inicial = {initial_balance}")
        assert initial_balance >= transfer_amount, "Error en la preparación: no hay suficientes fondos depositados para la prueba."

        # 3. Realizar la transferencia
        print(f"[Test] Transferencia BDI->BDI: Realizando transferencia de {transfer_amount} a {destination_phone}...")
        r_transfer = requests.post(transfer_url, json=payload, headers=headers, timeout=20) # Mayor timeout para llamadas externas
        r_transfer.raise_for_status()
        transfer_tx = r_transfer.json()
        print(f"[Test] Transferencia BDI->BDI: Respuesta recibida -> {transfer_tx}")

        assert transfer_tx.get("status") == "COMPLETED", \
             f"Estado de transferencia incorrecto. Esperado 'COMPLETED', recibido '{transfer_tx.get('status')}'"
        assert transfer_tx.get("amount") == transfer_amount, \
             f"Monto de transferencia incorrecto. Esperado {transfer_amount}, recibido {transfer_tx.get('amount')}"
        assert transfer_tx.get("destination_wallet_id") == destination_phone, \
             "ID de destino (teléfono) incorrecto en la transacción registrada."

        # 5. --- Verificación Crítica del Saldo ---
        final_balance = get_current_balance(auth_headers)
        print(f"[Test] Transferencia BDI->BDI: Saldo final = {final_balance}")
        expected_balance = initial_balance - transfer_amount
        assert final_balance == pytest.approx(expected_balance), \
               f"Saldo incorrecto después de la transferencia. Esperado ~{expected_balance}, recibido {final_balance}"

        print(f"[Test] Transferencia BDI->BDI: Saldo actualizado correctamente.")

    except requests.exceptions.Timeout:
         pytest.fail(f"Fallo en prueba de transferencia BDI->BDI: Timeout al llamar a {transfer_url}.")
    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else "Sin respuesta"
        pytest.fail(f"Fallo en prueba de transferencia BDI->BDI: Error en {transfer_url}. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}\nRespuesta: {error_text}")
    except (AssertionError, KeyError) as e:
        pytest.fail(f"Fallo en prueba de transferencia BDI->BDI: Verificación fallida - {e}")
    except Exception as e:
        pytest.fail(f"Fallo inesperado en prueba de transferencia BDI->BDI: {e}")


def test_transfer_insufficient_funds(auth_headers, idempotency_key):
    """
    Verifica que una transferencia BDI -> BDI falla con un error 400 (Bad Request)
    si el usuario no tiene fondos suficientes.
    """
    transfer_url = f"{GATEWAY_URL}/ledger/transfer"
    headers = {**auth_headers, "Idempotency-Key": idempotency_key}

    print(f"\n[Test] Transferencia Fondos Insuficientes: Verificando rechazo...")
    try:
        current_balance = get_current_balance(auth_headers)
        print(f"[Test] Transferencia Fondos Insuficientes: Saldo actual = {current_balance}")
        # Intentamos transferir más de lo que hay
        transfer_amount = current_balance + 100.0
        print(f"[Test] Transferencia Fondos Insuficientes: Intentando transferir {transfer_amount}...")

        payload = {
            "amount": transfer_amount,
            "to_bank": "HAPPY_MONEY",
            "destination_phone_number": "912345678" # Un número cualquiera
        }

        r_transfer = requests.post(transfer_url, json=payload, headers=headers, timeout=15)

        # Esperamos un error 400 Bad Request (devuelto por balance_service y propagado por ledger_service/gateway)
        assert r_transfer.status_code == 400, \
            f"Fallo en prueba de fondos insuficientes. Esperado status 400, recibido {r_transfer.status_code}. Respuesta: {r_transfer.text}"

        # --- Verificación Crítica: El saldo NO debe cambiar ---
        final_balance = get_current_balance(auth_headers)
        print(f"[Test] Transferencia Fondos Insuficientes: Saldo final = {final_balance}")
        assert final_balance == pytest.approx(current_balance), \
            f"El saldo cambió incorrectamente después de una transferencia fallida. Inicial: {current_balance}, Final: {final_balance}"

        print(f"[Test] Transferencia Fondos Insuficientes: Rechazo y saldo verificado correctamente.")

    except requests.exceptions.Timeout:
         pytest.fail(f"Fallo en prueba de fondos insuficientes: Timeout al llamar a {transfer_url}.")
    except requests.exceptions.RequestException as e:
        # Si el error NO es 400, la prueba falla
        if e.response is None or e.response.status_code != 400:
             error_text = e.response.text if e.response else "Sin respuesta"
             pytest.fail(f"Fallo en prueba de fondos insuficientes: Error inesperado en {transfer_url}. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}\nRespuesta: {error_text}")
        else:
             # Si SÍ es 400, la prueba pasa (verificamos saldo igualmente por si acaso)
             final_balance = get_current_balance(auth_headers)
             assert final_balance == pytest.approx(current_balance), \
                 f"El saldo cambió incorrectamente después de una transferencia fallida (error 400). Inicial: {current_balance}, Final: {final_balance}"
             print(f"[Test] Transferencia Fondos Insuficientes: Rechazo 400 recibido y saldo verificado correctamente.")
    except (AssertionError, KeyError) as e:
        pytest.fail(f"Fallo en prueba de fondos insuficientes: Verificación fallida - {e}")
    except Exception as e:
        pytest.fail(f"Fallo inesperado en prueba de fondos insuficientes: {e}")