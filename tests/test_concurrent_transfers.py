"""Prueba de concurrencia para verificar el manejo de transferencias simultáneas."""

import requests
import pytest
import time
import uuid
import threading # Para lanzar peticiones simultáneas
import logging

# Importar la URL base y fixtures desde conftest
from conftest import GATEWAY_URL

# Configurar logger
logger = logging.getLogger(__name__)

# --- Función ejecutada por cada Hilo ---
def transfer_thread(token: str, amount: float, destination: str, results: list, index: int):
    """
    Función que ejecuta una única solicitud de transferencia.
    Se usa para simular múltiples usuarios concurrentes.
    """
    transfer_url = f"{GATEWAY_URL}/ledger/transfer"
    # Cada hilo necesita su propia clave de idempotencia
    thread_idempotency_key = str(uuid.uuid4())
    headers = {
        "Authorization": f"Bearer {token}",
        "Idempotency-Key": thread_idempotency_key
    }
    payload = {
        "amount": amount,
        "to_bank": "HAPPY_MONEY",
        "destination_phone_number": destination
    }
    result_status = "UNKNOWN"
    try:
        r = requests.post(transfer_url, json=payload, headers=headers, timeout=20) # Mayor timeout
        # Guardamos el código de estado (o 0 si hay error de conexión)
        results[index] = r.status_code
        result_status = r.status_code
        # Loggear el resultado de cada hilo
        logger.info(f"Hilo {index}: Transferencia de {amount} -> Status {r.status_code} - Respuesta: {r.text[:100]}...") # Loguea los primeros 100 caracteres
    except requests.exceptions.Timeout:
        results[index] = "TIMEOUT"
        result_status = "TIMEOUT"
        logger.error(f"Hilo {index}: Timeout en la transferencia.")
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response else "CONN_ERROR"
        results[index] = status_code # Guardamos el código de error
        result_status = status_code
        logger.error(f"Hilo {index}: Error en la transferencia -> Status {status_code} - Error: {e}")
    except Exception as e:
         results[index] = "ERROR"
         result_status = "ERROR"
         logger.error(f"Hilo {index}: Error inesperado en el hilo -> {e}", exc_info=True)
    


# --- La Prueba Principal de Concurrencia ---
def test_concurrent_transfers_prevent_overdraft(test_user_token, auth_headers):
    """
    Verifica que el sistema previene sobregiros bajo carga concurrente.
    Lanza múltiples transferencias simultáneas que exceden el saldo
    y comprueba que solo un número apropiado de ellas tenga éxito (status 201).
    """
    deposit_url = f"{GATEWAY_URL}/ledger/deposit"
    balance_url = f"{GATEWAY_URL}/balance/me"
    initial_deposit = 1000.0 # Saldo inicial para la prueba
    num_threads = 5 # Número de transferencias simultáneas
    transfer_amount = 300.0 # Monto de cada transferencia
    # Saldo total: 1000. Monto por transferencia: 300.
    # Esperamos que solo 1000 / 300 = 3 transferencias tengan éxito.
    expected_success_count = int(initial_deposit // transfer_amount)

    print(f"\n[Test] Concurrencia: Preparando prueba con {num_threads} hilos transfiriendo {transfer_amount} c/u...")

    try:
        # 1. Asegurar saldo inicial
        print(f"[Test] Concurrencia: Depositando saldo inicial de {initial_deposit}...")
        deposit_key = str(uuid.uuid4())
        deposit_headers = {**auth_headers, "Idempotency-Key": deposit_key}
        requests.post(deposit_url, json={"amount": initial_deposit}, headers=deposit_headers, timeout=15).raise_for_status()
        time.sleep(1) # Pequeña pausa para asegurar que el depósito se procese
        initial_balance = requests.get(balance_url, headers=auth_headers, timeout=10).json()["balance"]
        print(f"[Test] Concurrencia: Saldo inicial confirmado = {initial_balance}")
        # Verificación rápida por si acaso
        assert initial_balance == pytest.approx(initial_deposit), "El depósito inicial falló o el saldo no es el esperado."

        # 2. Preparar y lanzar hilos
        threads = []
        results = [None] * num_threads # Lista para guardar el status_code de cada hilo
        destination_phone = "955555555" # Un número de prueba

        print(f"[Test] Concurrencia: Lanzando {num_threads} hilos...")
        for i in range(num_threads):
            # Creamos un hilo que ejecutará la función 'transfer_thread'
            t = threading.Thread(target=transfer_thread,
                                 args=(test_user_token['token'], transfer_amount, destination_phone, results, i))
            threads.append(t)
            t.start() # Iniciamos el hilo (ejecuta la transferencia)

        # 3. Esperar a que todos los hilos terminen
        print(f"[Test] Concurrencia: Esperando a que los {num_threads} hilos terminen...")
        for t in threads:
            t.join(timeout=30) # Espera máximo 30 segundos por hilo
            if t.is_alive():
                logger.warning(f"Un hilo de transferencia no terminó a tiempo.")
        print(f"[Test] Concurrencia: Todos los hilos han terminado.")
        print(f"[Test] Concurrencia: Resultados (códigos de estado): {results}")

        # 4. Verificar resultados
        # Contamos cuántas respuestas fueron 201 Created (éxito en nuestro ledger_service)
        success_count = sum(1 for status_code in results if status_code == 201)
        # Contamos cuántas fueron 400 Bad Request (fondos insuficientes)
        insufficient_funds_count = sum(1 for status_code in results if status_code == 400)

        print(f"[Test] Concurrencia: Transferencias exitosas (201) = {success_count}")
        print(f"[Test] Concurrencia: Rechazadas por fondos insuficientes (400) = {insufficient_funds_count}")

        # --- Verificación Crítica de Concurrencia ---
        # El número de éxitos DEBE ser exactamente el esperado (3 en este caso)
        assert success_count == expected_success_count, \
            f"Concurrencia fallida: Se esperaban {expected_success_count} transferencias exitosas, pero se obtuvieron {success_count}. ¡Posible sobregiro!"

        # El resto deberían haber fallado (idealmente por fondos insuficientes)
        # assert (success_count + insufficient_funds_count) <= num_threads # Permitir otros errores

        # --- Verificación Crítica del Saldo Final ---
        final_balance = requests.get(balance_url, headers=auth_headers, timeout=10).json()["balance"]
        print(f"[Test] Concurrencia: Saldo final = {final_balance}")
        # El saldo final debe ser el inicial menos el total de las transferencias exitosas
        expected_final_balance = initial_deposit - (success_count * transfer_amount)
        assert final_balance == pytest.approx(expected_final_balance), \
            f"Concurrencia fallida: Saldo final incorrecto. Esperado ~{expected_final_balance}, recibido {final_balance}."

        print(f"[Test] Concurrencia: Verificada correctamente. Sobregiro prevenido.")

    except requests.exceptions.Timeout:
         pytest.fail(f"Fallo en prueba de concurrencia: Timeout en operaciones.")
    except requests.exceptions.RequestException as e:
        error_text = e.response.text if e.response else "Sin respuesta"
        pytest.fail(f"Fallo en prueba de concurrencia: Error en petición. Status: {e.response.status_code if e.response else 'N/A'}. Error: {e}\nRespuesta: {error_text}")
    except AssertionError as e:
        pytest.fail(f"Fallo en prueba de concurrencia: Verificación fallida - {e}")
    except Exception as e:
        pytest.fail(f"Fallo inesperado en prueba de concurrencia: {e}")