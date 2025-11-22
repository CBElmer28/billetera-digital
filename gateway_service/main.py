"""API Gateway para Pixel Money. Punto de entrada único, maneja autenticación y enrutamiento."""

import os
import httpx
import logging
import time
import json
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException, status, Header, Depends
from fastapi.responses import JSONResponse, Response
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from typing import Optional 

# Carga variables de entorno
load_dotenv()

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- URLs de Servicios Internos ---
AUTH_URL = os.getenv("AUTH_SERVICE_URL")
BALANCE_URL = os.getenv("BALANCE_SERVICE_URL")
LEDGER_URL = os.getenv("LEDGER_SERVICE_URL")
GROUP_URL = os.getenv("GROUP_SERVICE_URL")

# --- CORRECCIÓN: Validar si se leyeron los valores correctamente ---
missing_urls = []
if not AUTH_URL: missing_urls.append("AUTH_SERVICE_URL")
if not BALANCE_URL: missing_urls.append("BALANCE_SERVICE_URL")
if not LEDGER_URL: missing_urls.append("LEDGER_SERVICE_URL")
if not GROUP_URL: missing_urls.append("GROUP_SERVICE_URL")

if missing_urls:
    logger.critical(f"Faltan URLs de servicios internos en .env: {', '.join(missing_urls)}")
    raise EnvironmentError(f"Faltan URLs de servicios internos: {', '.join(missing_urls)}")

# Inicializa FastAPI
app = FastAPI(
    title="API Gateway - Pixel Money",
    description="Punto de entrada único para todos los servicios de la billetera digital.",
    version="1.0.0"
)

# --- Configuración de CORS ---
origins = [
    "http://localhost",      # Para pruebas simples
    "http://localhost:3000", # Grafana (o Frontend si Grafana está apagado)
    "http://localhost:3001", # 👈 TU NUEVO PUERTO DE FRONTEND (OFICIAL)
    "http://localhost:3002", # Por si acaso
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True, 
    allow_methods=["*"],    
    allow_headers=["*"],    
)

# --- Rutas Públicas (no requieren token) ---
PUBLIC_ROUTES = [
    "/auth/login",
    "/auth/register",
    "/auth/verify-phone",
    "/auth/resend-code",
    "/health",
    "/metrics",
    "/docs",
    "/openapi.json",
    "/api/v1/inbound-transfer",
    "/bank/stats"  
    "/p2p/check"  # <--- AGREGA ESTA LÍNEA
]



# ... (cerca de PUBLIC_ROUTES) ...
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False) # auto_error=False para manejo manual
PARTNER_API_KEY = os.getenv("PARTNER_API_KEY")

async def get_api_key(api_key: str = Depends(api_key_header)):
    if not PARTNER_API_KEY:
         logger.error("PARTNER_API_KEY no está configurada en el Gateway .env")
         raise HTTPException(status_code=500, detail="Error de configuración interna del servidor.")
    if api_key != PARTNER_API_KEY:
        logger.warning(f"Intento de llamada a API de Partner con llave incorrecta: {api_key}")
        raise HTTPException(status_code=403, detail="API Key inválida o faltante")
    return api_key




# --- Cliente HTTP Asíncrono Reutilizable ---
client = httpx.AsyncClient(timeout=15.0)

# --- Métricas Prometheus ---
REQUEST_COUNT = Counter(
    "gateway_requests_total",
    "Total requests processed by API Gateway",
    ["method", "endpoint", "status_code"]
)
REQUEST_LATENCY = Histogram(
    "gateway_request_latency_seconds",
    "Request latency in seconds for API Gateway",
    ["endpoint"]
)

# --- Middlewares (Seguridad y Métricas) ---

@app.middleware("http")
async def combined_middleware(request: Request, call_next):
    """Middleware combinado para métricas y seguridad."""
    start_time = time.time()
    response = None
    status_code = 500
    user_id = None

    endpoint = request.url.path

    try:
        if request.method == "OPTIONS":
            response = await call_next(request)
            status_code = response.status_code 
            return response
        # --- Lógica de Seguridad (Autenticación) ---
        request.state.user_id = user_id # Inicializar
        is_public = any(request.url.path.startswith(p) for p in PUBLIC_ROUTES)

        if not is_public:
            token = request.headers.get("Authorization")
            if not token or not token.startswith("Bearer "):
                raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Cabecera Authorization ausente o inválida")

            token_value = token.split(" ")[1]
            try:
                verify_url = f"{AUTH_URL}/verify?token={token_value}"
                verify_response = await client.get(verify_url)

                if verify_response.status_code != 200:
                    detail = verify_response.json().get("detail", "Token inválido")
                    raise HTTPException(verify_response.status_code, detail)

                token_payload = verify_response.json()
                user_id_str = token_payload.get("sub")
                if user_id_str:
                    user_id = int(user_id_str)
                    request.state.user_id = user_id # Inyectamos user_id para los endpoints
                else:
                    raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Payload del token inválido (sin 'sub')")

            except httpx.RequestError:
                 raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Servicio de autenticación no disponible")
            except ValueError:
                 raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Payload del token inválido ('sub' no es un ID válido)")
            except Exception as auth_exc:
                 logger.error(f"Error inesperado en validación de token: {auth_exc}", exc_info=True)
                 raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Error en validación de token")

        # --- Llamar al siguiente middleware o endpoint ---
        response = await call_next(request)
        status_code = response.status_code

    except HTTPException as http_exc:
        # Captura errores HTTP lanzados por el middleware o el endpoint
        status_code = http_exc.status_code
        response = JSONResponse(status_code=status_code, content={"detail": http_exc.detail})
    
    except Exception as exc:
        # Captura excepciones no controladas
        logger.error(f"Middleware error inesperado en {endpoint}: {exc}", exc_info=True)
        response = JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
        status_code = 500
    finally:
        # --- Lógica de Métricas ---
        latency = time.time() - start_time
        final_status_code = getattr(response, 'status_code', status_code)
        REQUEST_LATENCY.labels(endpoint=endpoint).observe(latency)
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=final_status_code
        ).inc()

    if response is None: # Aseguramos que siempre haya una respuesta
         response = JSONResponse(status_code=500, content={"detail": "Internal Server Error"})

    return response


# --- Dependencia de Seguridad ---

async def get_current_user_id(request: Request) -> int:
    """
    Dependencia de FastAPI que extrae el user_id verificado por el middleware.
    Se usa en todos los endpoints protegidos.
    """
    user_id = getattr(request.state, "user_id", None)
    if user_id is None:
        # Esto no debería pasar si el middleware funciona, pero es una doble verificación.
        logger.error(f"Error crítico: get_current_user_id llamado en una ruta sin user_id autenticado ({request.url.path})")
        raise HTTPException(status.HTTP_403_FORBIDDEN, "User ID no disponible")
    return user_id


# --- Endpoints de Salud y Métricas ---
@app.get("/metrics", tags=["Monitoring"])
def metrics():
    """Expone métricas de la aplicación para Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/health", tags=["Monitoring"])
def health_check():
    """Verifica la salud básica del servicio Gateway."""
    return {"status": "ok", "service": "gateway_service"}

# --- Funciones Auxiliares para Proxy ---
async def forward_request(request: Request, target_url: str, inject_user_id: bool = False, pass_headers: list = []):
    """Función genérica para reenviar peticiones a servicios internos."""
    user_id = getattr(request.state, "user_id", None)
    
    payload = None
    headers_to_forward = {}

    for header_name in pass_headers:
        header_value = request.headers.get(header_name)
        if header_value:
            headers_to_forward[header_name] = header_value

    if user_id:
        headers_to_forward["X-User-Id"] = str(user_id)

    try:
        if request.method in ["POST", "PUT", "PATCH"]:
            content_type = request.headers.get("content-type", "").lower()
            
            if "application/json" in content_type:
                payload = await request.json()
                if inject_user_id:
                    if not user_id:
                        logger.error(f"Intento de inyectar user_id NULO en {target_url}")
                        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno: user_id no disponible")
                    payload['user_id'] = user_id
                response = await client.request(request.method, target_url, json=payload, headers=headers_to_forward)
            
            elif "application/x-www-form-urlencoded" in content_type:
                form_data = await request.form()
                response = await client.request(request.method, target_url, data=form_data, headers=headers_to_forward)
            
            else: 
                response = await client.request(request.method, target_url, content=await request.body(), headers=headers_to_forward)
        else: # GET, DELETE, etc.
            response = await client.request(request.method, target_url, headers=headers_to_forward)

        # Reenviar la respuesta (JSON o texto)
        try:
            response_json = response.json()
            return JSONResponse(status_code=response.status_code, content=response_json)
        except json.JSONDecodeError:
            return Response(status_code=response.status_code, content=response.text)

    except httpx.ConnectError as e:
        logger.error(f"Error de conexión al reenviar a {target_url}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, f"Servicio interno no disponible: {target_url}")
    except httpx.HTTPStatusError as e:
        # Propaga el error del servicio interno
        logger.warning(f"Servicio interno {target_url} devolvió error {e.response.status_code}: {e.response.text}")
        return Response(status_code=e.response.status_code, content=e.response.content)
    except Exception as e:
        logger.error(f"Error inesperado al reenviar a {target_url}: {e}", exc_info=True)
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "Error interno del Gateway")


# --- Endpoints Públicos (Proxy para Auth) ---

@app.post("/auth/register", tags=["Authentication"])
async def proxy_register(request: Request):
    """Reenvía la solicitud de registro al servicio de autenticación."""
    logger.info("Proxying request to /auth/register")
    return await forward_request(request, f"{AUTH_URL}/register")

@app.post("/auth/login", tags=["Authentication"])
async def proxy_login(request: Request):
    """Reenvía la solicitud de login (form-data) al servicio de autenticación."""
    logger.info("Proxying request to /auth/login")
    return await forward_request(request, f"{AUTH_URL}/login")

# --- NUEVOS ENDPOINTS PÚBLICOS DE VERIFICACIÓN ---

@app.post("/auth/verify-phone", tags=["Authentication"])
async def proxy_verify_phone(request: Request):
    """Reenvía la solicitud de verificación de código al servicio de autenticación."""
    logger.info("Proxying request to /auth/verify-phone")
    return await forward_request(request, f"{AUTH_URL}/verify-phone")

@app.post("/auth/resend-code", tags=["Authentication"])
async def proxy_resend_code(request: Request):
    """Reenvía la solicitud de reenvío de código al servicio de autenticación."""
    logger.info("Proxying request to /auth/resend-code")
    return await forward_request(request, f"{AUTH_URL}/resend-code")


# --- Endpoints Privados (Proxy para Auth) ---

@app.get("/auth/me", tags=["Authentication"])
async def proxy_get_my_profile(request: Request, user_id: int = Depends(get_current_user_id)):
    """Obtiene los datos del usuario autenticado (proxy hacia auth_service)."""
    logger.info(f"Proxying request to /users/{user_id}")
    return await forward_request(request, f"{AUTH_URL}/users/{user_id}")

@app.post("/auth/change-password", tags=["Authentication"])
async def proxy_change_password(request: Request, user_id: int = Depends(get_current_user_id)):
    """
    Proxy que permite a un usuario autenticado cambiar su contraseña.
    Reenvía la solicitud al endpoint:
      POST auth_service/users/{user_id}/change-password
    """

    logger.info(f"Proxying request to /users/{user_id}/change-password")

    return await forward_request(
        request,
        f"{AUTH_URL}/users/{user_id}/change-password"
    )

# --- Endpoints Privados (Proxy para Balance) ---

@app.get("/balance/me", tags=["Balance"])
async def proxy_get_my_balance(request: Request, user_id: int = Depends(get_current_user_id)):
    """Obtiene el saldo del usuario autenticado."""
    logger.info(f"Proxying request to /balance/{user_id}")
    return await forward_request(request, f"{BALANCE_URL}/balance/{user_id}")

# --- Endpoints Privados (Proxy para Ledger) ---

# (¡Borra el endpoint 'proxy_deposit'!)

@app.post("/request-loan", tags=["Ledger", "BDI Préstamos"])
async def proxy_request_loan(
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un usuario solicite un préstamo."""
    logger.info(f"Proxying request to /request-loan for user_id: {user_id}")

    # Esta llamada SÍ necesita el X-User-ID (que 'forward_request' añade)
    return await forward_request(
        request, 
        f"{BALANCE_URL}/request-loan", # Llama al nuevo endpoint
        inject_user_id=False,
        pass_headers=["Authorization", "Idempotency-Key"] # Pasamos la Idempotency-Key
    )

# ... (después de 'proxy_request_loan')

@app.post("/pay-loan", tags=["Ledger", "BDI Préstamos"])
async def proxy_pay_loan(
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un usuario pague su préstamo."""
    logger.info(f"Proxying request to /pay-loan for user_id: {user_id}")

    # Esta llamada SÍ necesita el X-User-ID
    return await forward_request(
        request, 
        f"{BALANCE_URL}/pay-loan",
        inject_user_id=False,
        pass_headers=["Authorization", "Idempotency-Key"] 
    )


@app.post("/ledger/transfer", tags=["Ledger"])
async def proxy_transfer(request: Request, user_id: int = Depends(get_current_user_id)):
    """Reenvía la solicitud de transferencia al servicio de ledger, inyectando user_id."""
    logger.info(f"Proxying request to /ledger/transfer for user_id: {user_id}")
    return await forward_request(request, f"{LEDGER_URL}/transfer", inject_user_id=True, pass_headers=["Idempotency-Key", "Authorization"])

@app.post("/ledger/contribute", tags=["Ledger"])
async def proxy_contribute(request: Request, user_id: int = Depends(get_current_user_id)):
    """Reenvía la solicitud de aporte a grupo al servicio de ledger, inyectando user_id."""
    logger.info(f"Proxying request to /ledger/contribute for user_id: {user_id}")
    return await forward_request(request, f"{LEDGER_URL}/contribute", inject_user_id=True, pass_headers=["Idempotency-Key", "Authorization"])

# ... (después de @app.post("/ledger/contribute") ...)

@app.post("/ledger/transfer/p2p", tags=["Ledger"])
async def proxy_transfer_p2p(request: Request, user_id: int = Depends(get_current_user_id)):
    """Reenvía la solicitud de transferencia P2P, inyectando el user_id (remitente)."""
    logger.info(f"Proxying request to /ledger/transfer/p2p for user_id: {user_id}")
    return await forward_request(
        request, 
        f"{LEDGER_URL}/transfer/p2p", 
        inject_user_id=True,
        pass_headers=["Idempotency-Key", "Authorization"]
    )


@app.get("/ledger/transactions/me", tags=["Ledger"])
async def proxy_get_my_transactions(request: Request, user_id: int = Depends(get_current_user_id)):
    """Obtiene el historial de movimientos del usuario autenticado."""
    logger.info(f"Proxying request to /ledger/transactions/me for user_id: {user_id}")

    
    # El user_id se pasa por el header X-User-ID
    return await forward_request(
        request, 
        f"{LEDGER_URL}/transactions/me",
        pass_headers=["Authorization"]
    )

# ... (después de 'proxy_get_my_transactions')

@app.get("/ledger/transactions/group/{group_id}", tags=["Ledger"])
async def proxy_get_group_transactions(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Obtiene el historial de movimientos de un grupo (BDG)."""
    logger.info(f"Proxying request to /ledger/transactions/group/{group_id} for user_id: {user_id}")

    # (El X-User-ID se añade automáticamente, el ledger_service lo usará para seguridad)
    return await forward_request(
        request, 
        f"{LEDGER_URL}/transactions/group/{group_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )


@app.get("/ledger/analytics/daily_balance/me", tags=["Ledger Analytics"])
async def proxy_get_my_daily_balance(request: Request, user_id: int = Depends(get_current_user_id)):
    """
    Obtiene el historial de balance diario del usuario autenticado (últimos 30 días).
    Proxy hacia el Ledger Service.
    """
    logger.info(f"Proxying request to /ledger/analytics/daily_balance/me for user_id: {user_id}")

    # Construimos la URL del servicio Ledger
    target_url = f"{LEDGER_URL}/analytics/daily_balance/{user_id}"

    # Reenviamos la solicitud al Ledger Service
    return await forward_request(
        request,
        target_url,
        pass_headers=["Authorization"]
    )
# --- Endpoints Privados (Proxy para Group) ---

@app.post("/groups", status_code=status.HTTP_201_CREATED, tags=["Groups"])
async def proxy_create_group(request: Request, user_id: int = Depends(get_current_user_id)):
    """
    Reenvía la solicitud de creación de grupo al servicio de grupos,
    inyectando el user_id del token verificado.
    """
    logger.info(f"Proxying request to /groups for user_id: {user_id}")
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups", 
        inject_user_id=False, 
        pass_headers=["Authorization"]
    )

@app.get("/groups/me", tags=["Groups"])
async def proxy_get_my_groups(request: Request, user_id: int = Depends(get_current_user_id)):
    """Obtiene los grupos del usuario autenticado."""
    logger.info(f"Proxying request to /groups/me for user_id: {user_id}")

   
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/me",
        inject_user_id=False, 
        pass_headers=["Authorization"]
    )

# ... (después de la función proxy_get_my_groups) ...

@app.post("/groups/me/accept/{group_id}", tags=["Groups"])
async def proxy_accept_invite(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un usuario acepte una invitación a un grupo."""
    logger.info(f"Proxying request to /groups/me/accept/{group_id} for user_id: {user_id}")

    # Esta llamada necesita el X-User-ID (que 'forward_request' añade)
    # pero no tiene payload (inject_user_id=False).
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/me/accept/{group_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )

# ... (después de la función proxy_accept_invite) ...

@app.delete("/groups/me/reject/{group_id}", tags=["Groups"])
async def proxy_reject_invite(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un usuario rechace una invitación a un grupo."""
    logger.info(f"Proxying request to /groups/me/reject/{group_id} for user_id: {user_id}")

    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/me/reject/{group_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )



@app.post("/groups/{group_id}/invite", tags=["Groups"])
async def proxy_invite_member(group_id: int, request: Request, user_id: int = Depends(get_current_user_id)):
    """Reenvía la solicitud de invitación de miembro al servicio de grupos."""
    logger.info(f"Proxying request to /groups/{group_id}/invite for user_id: {user_id}")
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/invite", 
        inject_user_id=False, 
        pass_headers=["Authorization"]
    )



@app.get("/groups/{group_id}", tags=["Groups"])
async def proxy_get_group(group_id: int, request: Request, user_id: int = Depends(get_current_user_id)):
    """Reenvía la solicitud para obtener detalles de un grupo."""
    logger.info(f"Proxying request to /groups/{group_id} for user_id: {user_id}")
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}", 
        inject_user_id=False, 
        pass_headers=["Authorization"]
    )

# ... (después de la función proxy_get_my_groups) ...

@app.get("/group_balance/{group_id}", tags=["Groups", "Balance"])
async def proxy_get_group_balance(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """
    Obtiene el saldo de una cuenta de grupo (BDG).
    Proxy hacia balance_service.
    """
    logger.info(f"Proxying request to /group_balance/{group_id} for user_id: {user_id}")

    # Esta llamada necesita el X-User-ID (que 'forward_request' añade)
    # para que balance_service pueda (en el futuro) verificar si eres miembro.
    return await forward_request(
        request, 
        f"{BALANCE_URL}/group_balance/{group_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )


# ... (después de la función proxy_get_group_balance) ...

@app.delete("/groups/{group_id}/kick/{user_id_to_kick}", tags=["Groups"])
async def proxy_kick_member(
    group_id: int, 
    user_id_to_kick: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un líder elimine a un miembro."""
    logger.info(f"Proxying KICK request for group {group_id}, target {user_id_to_kick}, by leader {user_id}")
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/kick/{user_id_to_kick}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )

@app.delete("/groups/me/leave/{group_id}", tags=["Groups"])
async def proxy_leave_group(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un miembro se salga de un grupo."""
    logger.info(f"Proxying LEAVE request for group {group_id} by member {user_id}")
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/me/leave/{group_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )


# ... (después de la función proxy_leave_group) ...

@app.delete("/groups/{group_id}", tags=["Groups"])
async def proxy_delete_group(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un líder elimine su grupo."""
    logger.info(f"Proxying DELETE request for group {group_id} by leader {user_id}")
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )

# ... (después de la sección de Grupos) ...
# ... (después de la función proxy_delete_group) ...

@app.post("/groups/{group_id}/request-withdrawal", tags=["Groups", "Junta (Retiros)"])
async def proxy_create_withdrawal_request(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un miembro solicite un retiro de fondos del grupo."""
    logger.info(f"Proxying request to /groups/{group_id}/request-withdrawal for user_id: {user_id}")

    # El X-User-ID (del miembro) se añade automáticamente
    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/request-withdrawal",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )
# --- API Externa v1 (Para Partners como Vercel) ---

# ... (después de 'proxy_create_withdrawal_request') ...

@app.post("/groups/{group_id}/approve-withdrawal/{request_id}", tags=["Groups", "Junta (Retiros)"])
async def proxy_approve_withdrawal_request(
    group_id: int, 
    request_id: int,
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un LÍDER apruebe una solicitud de retiro."""
    logger.info(f"Proxying request to /groups/{group_id}/approve-withdrawal/{request_id} for LÍDER {user_id}")

    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/approve-withdrawal/{request_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )


# ... (después de 'proxy_create_withdrawal_request') ...

@app.post("/groups/{group_id}/reject-withdrawal/{request_id}", tags=["Groups", "Junta (Retiros)"])
async def proxy_reject_withdrawal_request(
    group_id: int, 
    request_id: int,
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un LÍDER rechace una solicitud de retiro."""
    logger.info(f"Proxying request to /groups/{group_id}/reject-withdrawal/{request_id} for LÍDER {user_id}")

    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/reject-withdrawal/{request_id}",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )

@app.get("/groups/{group_id}/withdrawal-requests", tags=["Groups", "Junta (Retiros)"])
async def proxy_get_withdrawal_requests(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un LÍDER vea la lista de solicitudes de retiro."""
    logger.info(f"Proxying request to /groups/{group_id}/withdrawal-requests for LÍDER {user_id}")

    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/withdrawal-requests",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )


# ... (después de 'proxy_get_withdrawal_requests') ...

@app.post("/groups/{group_id}/leader-withdrawal", tags=["Groups", "Junta (Retiros)"])
async def proxy_leader_withdrawal(
    group_id: int, 
    request: Request, 
    user_id: int = Depends(get_current_user_id)
):
    """Proxy para que un LÍDER ejecute un retiro directo."""
    logger.info(f"Proxying request to /groups/{group_id}/leader-withdrawal for LÍDER {user_id}")

    return await forward_request(
        request, 
        f"{GROUP_URL}/groups/{group_id}/leader-withdrawal",
        inject_user_id=False,
        pass_headers=["Authorization"]
    )


@app.post("/api/v1/inbound-transfer", tags=["Partner API"])
async def partner_inbound_transfer(
    request: Request,
    api_key: str = Depends(get_api_key) # ¡Seguridad!
):
    """
    Punto de entrada público para que partners (ej. Otro Grupo) 
    depositen dinero a un usuario de Pixel Money via número de celular.
    """
    logger.info(f"Recibida llamada de Partner API a /api/v1/inbound-transfer")

    return await forward_request(
        request, 
        f"{LEDGER_URL}/transfers/inbound",
        inject_user_id=False,
        pass_headers=[] # No pasamos ningún header del partner
    )

# Agrega esto al final de gateway_service/main.py

@app.get("/bank/stats", tags=["Bank Admin"])
async def proxy_bank_stats(request: Request):
    """Proxy para ver las ganancias del banco (Balance Service)."""
    return await forward_request(request, f"{BALANCE_URL}/bank/stats")




@app.get("/p2p/check/{phone_number}", tags=["P2P"])
async def check_recipient_name(
    phone_number: str, 
    request: Request, 
):
    """Permite al frontend validar el nombre del destinatario antes de transferir."""
    # Reenvía la consulta al Auth Service
    return await forward_request(request, f"{AUTH_URL}/users/by-phone/{phone_number}")



@app.delete("/auth/me", tags=["Authentication"])
async def proxy_delete_me(request: Request, user_id: int = Depends(get_current_user_id)):
    """Elimina la cuenta del usuario actual (si no tiene deudas)."""
    logger.info(f"Solicitud de eliminación de cuenta para user_id: {user_id}")
    # Redirigimos al Auth Service endpoint /users/{id}
    return await forward_request(request, f"{AUTH_URL}/users/{user_id}")


# --- Manejador de Cierre ---
@app.on_event("shutdown")
async def shutdown_event():
    """Cierra el cliente HTTP al apagar la aplicación."""
    await client.aclose()
    logger.info("Cliente HTTP del Gateway cerrado.")