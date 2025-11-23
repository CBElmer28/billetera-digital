# Billetera-Digital/balance_service/schemas.py (Corregido y Completo)

"""Modelos Pydantic (schemas) para el Balance Service."""

from pydantic import BaseModel, Field, ConfigDict
from decimal import Decimal       # <-- ¡Importación clave!
from datetime import datetime     # <-- ¡Importación clave!
from typing import Optional       # <-- ¡Importación clave!
from models import LoanStatus     # <-- ¡Importación clave!

# --- Schemas de Cuenta Individual (BDI) ---

class AccountCreate(BaseModel):
    user_id: int

class BalanceUpdate(BaseModel):
    user_id: int
    amount: float # El 'float' se convertirá a Decimal en main.py

class BalanceCheck(BaseModel):
    user_id: int
    amount: float

class DepositRequest(BaseModel):
    """Schema para el modal de Préstamo (reusado)."""
    amount: float = Field(..., gt=0) # gt=0 significa "greater than 0"
    dni: Optional[str] = None

# --- ¡CLASE QUE FALTABA! ---
class LoanResponse(BaseModel):
    """Schema para mostrar un préstamo activo."""
    id: int
    outstanding_balance: Decimal
    status: LoanStatus
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)
# --- FIN DE CLASE QUE FALTABA ---

class AccountResponse(BaseModel):
    user_id: int
    balance: Decimal
    version: int
    
    loan: Optional[LoanResponse] = None 

    model_config = ConfigDict(from_attributes=True)

# --- Schemas de Cuenta Grupal (BDG) ---

class GroupAccountCreate(BaseModel):
    group_id: int

class GroupBalanceUpdate(BaseModel):
    group_id: int
    amount: float

class GroupAccount(BaseModel):
    group_id: int
    balance: Decimal
    version: int
    
    model_config = ConfigDict(from_attributes=True)