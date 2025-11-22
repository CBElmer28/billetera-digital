"""Modelos Pydantic (schemas) para el Balance Service."""

from pydantic import BaseModel, Field, ConfigDict
from decimal import Decimal
from datetime import datetime
from typing import Optional
from models import LoanStatus

# --- Schemas de Cuenta Individual (BDI) ---

class AccountCreate(BaseModel):
    user_id: int

class BalanceUpdate(BaseModel):
    user_id: int
    amount: float 

class BalanceCheck(BaseModel):
    user_id: int
    amount: float

class DepositRequest(BaseModel):
    """
    Schema para solicitar préstamo.
    Fusión: Amount (Develop) + DNI (Stress-Test).
    """
    amount: float = Field(..., gt=0)
    # Agregamos DNI como opcional para compatibilidad, pero el endpoint lo usará
    dni: Optional[str] = None 

class LoanResponse(BaseModel):
    """Schema para mostrar un préstamo activo."""
    id: int
    outstanding_balance: Decimal
    status: LoanStatus
    created_at: datetime
    # Agregamos DNI a la respuesta por si el front lo necesita
    dni: Optional[str] = None 
    
    model_config = ConfigDict(from_attributes=True)

class AccountResponse(BaseModel):
    """Respuesta principal para /balance/me."""
    user_id: int
    balance: Decimal
    version: int
    
    # IMPORTANTE: Usamos 'loan' en lugar de 'active_loan'.
    # Razón: En models.py la relación se llama 'loan'. 
    # Al usar from_attributes=True, Pydantic busca active_account.loan automágicamente.
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