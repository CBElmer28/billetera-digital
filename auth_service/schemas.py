"""Modelos Pydantic (schemas) para validación de datos de entrada/salida en el Servicio de Autenticación."""

from pydantic import BaseModel, EmailStr, Field, ConfigDict
from typing import Optional, List

# --- Schemas de Usuario ---

class UserCreate(BaseModel):
    """Schema para los datos requeridos al crear un nuevo usuario."""
    name: str = Field(..., min_length=3, description="Nombre completo del usuario")
    email: EmailStr # <-- CORREGIDO (era 'str')
    password: str = Field(..., min_length=8, description="La contraseña debe tener al menos 8 caracteres")
    phone_number: str = Field(..., min_length=9, max_length=15)

class UserResponse(BaseModel):
    """Schema para los datos devueltos tras la creación exitosa de un usuario (excluye contraseña)."""
    id: int
    name: str
    email: EmailStr # <-- CORREGIDO (era 'str')
    phone_number: str | None = None

    # Configuración de Pydantic v2+ para permitir mapeo desde modelos ORM (SQLAlchemy)
    model_config = ConfigDict(from_attributes=True)


# --- Schemas de Token ---

class Token(BaseModel):
    """Schema para el token de acceso JWT devuelto tras un login exitoso."""
    access_token: str
    token_type: str = "bearer"
    user_id: int      # <-- ¡AÑADE ESTO!
    name: str         # <-- ¡AÑADE ESTO!
    email: EmailStr   # <-- ¡AÑADE ESTO!

class TokenPayload(BaseModel):
    """Schema que representa el payload decodificado de un token JWT válido."""
    
    # 'sub' (subject) es el campo estándar de JWT para guardar el ID de usuario
    sub: Optional[str] = None
    exp: Optional[int] = None
    name: Optional[str] = None

class UserBulkRequest(BaseModel):
    user_ids: List[int]

class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str
    confirm_password: str # (Opcional, para validar si quieres, pero con current y new basta)