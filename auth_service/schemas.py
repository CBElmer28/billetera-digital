"""Modelos Pydantic (schemas) para validación de datos de entrada/salida en el Servicio de Autenticación."""

from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List

# --- Schemas de Usuario ---

class UserCreate(BaseModel):
    """
    Schema modificado: Pide DNI, NO pide nombre.
    """
    dni: str = Field(..., min_length=8, max_length=8, description="DNI del usuario")
    email: EmailStr
    password: str = Field(..., min_length=8)
    phone_number: str = Field(..., min_length=9, max_length=15)

class UserResponse(BaseModel):
    id: int
    dni: str # <--- Agregamos esto
    name: str # Auth Service lo llenará automáticamente
    email: EmailStr
    phone_number: str | None = None
    central_wallet_id: Optional[str] = None 

    model_config = ConfigDict(from_attributes=True)


# --- Schemas de Token ---

class Token(BaseModel):
    """Schema para el token de acceso JWT devuelto tras un login exitoso."""
    access_token: str
    token_type: str = "bearer"
    user_id: int      
    name: str         
    email: EmailStr   

class TokenPayload(BaseModel):
    """Schema que representa el payload decodificado de un token JWT válido."""
    sub: Optional[str] = None
    exp: Optional[int] = None
    name: Optional[str] = None

class UserBulkRequest(BaseModel):
    user_ids: List[int]

class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str
    confirm_password: str


class PasswordCheck(BaseModel):
    """Schema para verificar contraseña sin loguear."""
    password: str