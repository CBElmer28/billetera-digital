from sqlalchemy import Column, Integer, String, Boolean, DateTime
from db import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    phone_number = Column(String(20), unique=True, index=True, nullable=True)
    
    # --- CAMPOS CRÍTICOS DE DEVELOP ---
    # Son necesarios porque el código main.py los usa.
    # Incluso en "Stress Mode", el código inserta estos valores (aunque no envíe el SMS).
    
    telegram_chat_id = Column(String(100), unique=True, index=True, nullable=False)

    hashed_password = Column(String(255), nullable=False)
    is_active = Column(Boolean, default=True)

    # --- CAMPOS DE VERIFICACIÓN ---
    is_phone_verified = Column(Boolean, default=False, nullable=False)
    phone_verification_code = Column(String(6), nullable=True)
    phone_verification_expires = Column(DateTime, nullable=True)