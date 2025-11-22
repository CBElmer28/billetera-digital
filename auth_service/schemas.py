"""Modelos Pydantic (schemas) para validación de datos de entrada/salida en el Servicio de Autenticación."""

from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List
from datetime import datetime

# --- Schemas de Usuario ---

class UserCreate(BaseModel):
    """Schema para los datos requeridos al crear un nuevo usuario."""
    name: str = Field(..., min_length=3, description="Nombre completo del usuario")
    email: EmailStr
    password: str = Field(..., min_length=8, description="La contraseña debe tener al menos 8 caracteres")
    phone_number: str = Field(..., min_length=9, max_length=15)
    
    # --- MODIFICACIÓN HÍBRIDA ---
    # Lo hacemos opcional para que los scripts de stress-test (que no envían esto) pasen la validación inicial.
    # En main.py, si NO estamos en modo estrés, validamos manualmente que esto exista.
    telegram_chat_id: Optional[str] = Field(None, min_length=5, description="ID de Chat de Telegram del usuario")

class UserResponse(BaseModel):
    """Schema para los datos devueltos tras la creación exitosa de un usuario (excluye contraseña)."""
    id: int
    name: str
    email: EmailStr
    phone_number: str | None = None

    # Configuración de Pydantic v2+ para permitir mapeo desde modelos ORM (SQLAlchemy)
    model_config = ConfigDict(from_attributes=True)


# --- Schemas de Token ---

class Token(BaseModel):
    """Schema para el token de acceso JWT devuelto tras un login exitoso."""
    access_token: str
    token_type: str = "bearer"
    user_id: int      
    name: str         
    email: EmailStr   
    # Mantenemos esto de develop para que el frontend sepa qué hacer
    is_phone_verified: bool 

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

# --- Schemas de Verificación de Teléfono (De Develop) ---

class PhoneVerificationRequest(BaseModel):
    """Schema para verificar un código de teléfono."""
    phone_number: str = Field(..., description="Número de teléfono que se está verificando")
    code: str = Field(..., min_length=6, max_length=6, description="Código de 6 dígitos")

class RequestVerificationCode(BaseModel):
    """Schema para solicitar un nuevo código (reenvío)."""
    phone_number: str