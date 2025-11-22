import enum
from decimal import Decimal
from sqlalchemy import Column, Integer, String, Float, UniqueConstraint, ForeignKey, Numeric, DateTime, func, Enum as SQLEnum
from sqlalchemy.orm import relationship

from db import Base

class Account(Base):
    """
    Modelo SQLAlchemy que representa la tabla 'accounts' (BDI).
    """
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    # Mantenemos unique=True de develop para consistencia
    user_id = Column(Integer, unique=True, index=True, nullable=False)
    
    # Usamos Numeric/Decimal por seguridad financiera (Develop)
    balance = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))
    version = Column(Integer, nullable=False, default=1, server_default='1')
    currency = Column(String(10), nullable=False, default="PEN")

    # Relación 1 a 1 con el préstamo activo
    loan = relationship("Loan", uselist=False, back_populates="account", primaryjoin="Account.user_id == Loan.user_id")


class GroupAccount(Base):
    """Modelo de la cuenta de una Billetera Grupal (BDG)."""
    __tablename__ = "group_accounts"

    group_id = Column(Integer, primary_key=True, index=True) 
    balance = Column(Numeric(10, 2), nullable=False, default=0.00)
    version = Column(Integer, nullable=False, default=1) 

    __mapper_args__ = {
        "version_id_col": version
    }

class LoanStatus(str, enum.Enum):
    """Define el estado de un préstamo."""
    ACTIVE = "active"
    PAID = "paid"

class Loan(Base):
    """
    Modelo para la tabla 'loans'.
    Fusión: Estructura Develop + Campo DNI de Stress-Test.
    """
    __tablename__ = "loans"

    id = Column(Integer, primary_key=True, index=True)
    # Mantenemos la restricción estricta de Develop (Un usuario, una cuenta)
    user_id = Column(Integer, ForeignKey("accounts.user_id"), nullable=False) 

    # --- CAMPO NUEVO (Del Stress-Test) ---
    # Necesario para guardar el DNI validado con RENIEC
    dni = Column(String(8), nullable=True) 
    
    principal_amount = Column(Numeric(10, 2), nullable=False)
    outstanding_balance = Column(Numeric(10, 2), nullable=False)
    interest_rate = Column(Numeric(5, 2), nullable=False, default=Decimal('5.00'))

    status = Column(SQLEnum(LoanStatus), nullable=False, default=LoanStatus.ACTIVE)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Mantenemos due_date de Develop (útil para el futuro), aunque Stress lo quitó
    due_date = Column(DateTime(timezone=True), nullable=True) 

    account = relationship("Account", back_populates="loan", foreign_keys=[user_id])