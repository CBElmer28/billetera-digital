# group_service/schemas.py (Versión Corregida y Limpia)

"""Modelos Pydantic (schemas) para validación de datos en el Group Service."""

from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, List
from models import GroupRole, GroupMemberStatus, WithdrawalRequest, WithdrawalRequestStatus # ¡La importación clave!
from decimal import Decimal

# --- Schemas de Entrada (Input) ---

class GroupCreate(BaseModel):
    """Schema para crear un grupo. El líder viene por Header."""
    name: str = Field(..., min_length=3, max_length=100)
    # El user_id (líder) vendrá por Header (X-User-ID), NO aquí.

class GroupInviteRequest(BaseModel):
    """Schema para invitar. El invitador viene por Header."""
    phone_number_to_invite: str = Field(..., description="Celular del usuario a invitar")
    # El user_id (invitador) vendrá por Header (X-User-ID), NO aquí.


# --- Schemas de Salida (Respuesta) ---

class GroupMemberResponse(BaseModel):
    """
    Schema para representar la información de un miembro dentro de un grupo.
    Usado para las listas anidadas (ej. group.members).
    ¡ESTA ES LA ÚNICA DEFINICIÓN!
    """
    user_id: int
    name: str = "Nombre no encontrado"
    role: GroupRole # Muestra el rol ('leader' o 'member')
    group_id: int  # <-- El campo que faltaba en la definición duplicada
    status: GroupMemberStatus
    internal_balance: Decimal

    # Configuración Pydantic v2+ para mapeo desde modelos ORM
    model_config = ConfigDict(from_attributes=True)

class GroupResponse(BaseModel):
    """Schema para mostrar un grupo completo."""
    id: int
    name: str
    leader_user_id: int
    created_at: datetime # <-- ¡AÑADE ESTA LÍNEA!
    members: List[GroupMemberResponse] = []

    model_config = ConfigDict(from_attributes=True)

class InternalBalanceUpdate(BaseModel):
    user_id_to_update: int
    amount: float # Puede ser positivo (aporte) o negativo (retiro)

# ... (al final del archivo)

class WithdrawalRequestCreate(BaseModel):
    """Schema para la solicitud de un miembro para retirar fondos."""
    amount: float = Field(..., gt=0, description="Monto a retirar")
    reason: Optional[str] = Field(None, max_length=255, description="Razón del retiro")

class WithdrawalRequestResponse(BaseModel):
    """Schema para mostrar una solicitud de retiro."""
    id: int
    group_id: int
    member_user_id: int
    amount: Decimal
    reason: Optional[str] = None
    status: WithdrawalRequestStatus
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)

class LeaderWithdrawalRequest(BaseModel):
    """Schema para la solicitud de un LÍDER para retirar fondos."""
    amount: float = Field(..., gt=0, description="Monto a retirar")
    reason: Optional[str] = Field(None, max_length=255, description="Razón del retiro")