"""Modelos Pydantic (schemas) para validación de datos de entrada/salida en el Ledger Service."""

from pydantic import BaseModel, Field, UUID4, ConfigDict
from datetime import datetime
from typing import Optional

# --- Esquemas de Entrada (Input) ---

class DepositRequest(BaseModel):
    """Schema para la solicitud de depósito en una BDI."""
    # user_id será inyectado por el Gateway desde el token JWT.
    user_id: int
    amount: float = Field(..., gt=0, description="El monto a depositar debe ser positivo.")

class TransferRequest(BaseModel):
    """Schema para la solicitud de transferencia BDI -> BDI (externa)."""
    # user_id será inyectado por el Gateway.
    user_id: int 
    amount: float = Field(..., gt=0, description="El monto a transferir debe ser positivo.")
    to_bank: str = Field(..., description="Banco destino (ej. 'HAPPY_MONEY')")
    # Identificador del destinatario en el otro banco (número de celular).
    destination_phone_number: str = Field(..., min_length=9, max_length=15, description="Número de celular del destinatario.")

class ContributionRequest(BaseModel):
    """Schema para la solicitud de aporte BDI -> BDG (Billetera Grupal)."""
    # user_id será inyectado por el Gateway (es quien aporta).
    user_id: int
    group_id: int = Field(..., description="ID del grupo (BDG) que recibe el aporte.")
    amount: float = Field(..., gt=0, description="El monto a aportar debe ser positivo.")

# --- Esquema de Salida (Respuesta) ---



class P2PTransferRequest(BaseModel):
    """Schema para la solicitud de transferencia P2P (BDI -> BDI)."""
    # user_id (quien envía) vendrá del Gateway.
    user_id: int 
    amount: float = Field(..., gt=0)
    destination_phone_number: str = Field(..., min_length=9, max_length=15)

class Transaction(BaseModel):
    """
    Schema para representar una transacción completada, tal como se almacena
    y se devuelve al cliente.
    """
    id: UUID4
    user_id: int
    source_wallet_type: Optional[str] = None
    source_wallet_id: Optional[str] = None
    destination_wallet_type: Optional[str] = None
    destination_wallet_id: Optional[str] = None
    type: str
    amount: float
    currency: Optional[str] = None
    status: str
    created_at: datetime
    updated_at: datetime
    metadata: Optional[str] = None # Almacenado como un string JSON

    # Configuración Pydantic v2+ para mapear desde objetos de base de datos (ORM/Cassandra).
    model_config = ConfigDict(from_attributes=True)


# En ledger_service/schemas.py

class InboundTransferRequest(BaseModel):
    """Schema para recibir dinero de un banco externo (API v1)."""
    destination_phone_number: str = Field(..., min_length=9, max_length=15)
    amount: float = Field(..., gt=0)
    external_transaction_id: str # El ID de la transacción del "otro banco"

# ... (al final del archivo)

class GroupWithdrawalRequest(BaseModel):
    """Schema interno para que el ledger procese un retiro de grupo."""
    group_id: int
    member_user_id: int # El ID del miembro que RECIBIRÁ el dinero
    amount: float
    request_id: int # El ID de la 'withdrawal_request' (de la BD de MariaDB)



class LoanEventRequest(BaseModel):
    """Schema para procesar desembolsos o pagos de préstamos iniciados por Balance Service."""
    user_id: int
    amount: float = Field(..., gt=0)
    loan_id: int # Para referenciar el préstamo en los metadatos