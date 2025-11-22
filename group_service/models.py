"""Define los modelos de las tablas 'groups' y 'group_members' usando SQLAlchemy ORM."""

import enum
from sqlalchemy import Column, Integer, String, ForeignKey, Enum as SQLEnum , DateTime, func, Numeric
from sqlalchemy.orm import relationship
from decimal import Decimal
# Importación absoluta desde el módulo db.py del mismo directorio
from db import Base

class GroupRole(str, enum.Enum):
    """Define los roles posibles para un miembro dentro de un grupo."""
    LEADER = "leader" # Rol de líder/administrador del grupo
    MEMBER = "member" # Rol de miembro estándar

class Group(Base):
    """
    Modelo SQLAlchemy que representa la tabla 'groups'.
    Almacena la información básica de una Billetera Digital Grupal (BDG).
    """
    __tablename__ = "groups"

    # Clave primaria autoincremental del grupo
    id = Column(Integer, primary_key=True, index=True)
    # Nombre del grupo 
    name = Column(String(100), nullable=False, index=True)
    # ID del usuario (del auth_service) que creó y lidera el grupo
    leader_user_id = Column(Integer, nullable=False, index=True)

    # Relación uno-a-muchos con GroupMember.
    # Permite acceder a group.members para obtener la lista de miembros.
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    members = relationship("GroupMember", back_populates="group")

# Reemplaza la clase GroupMember con esto:
class GroupMemberStatus(str, enum.Enum):
    """Define el estado de la membresía de un usuario en un grupo."""
    PENDING = "pending"
    ACTIVE = "active"
    # (En el futuro: REJECTED, BANNED, etc.)

# ... (después de class GroupMemberStatus...)

class WithdrawalRequestStatus(str, enum.Enum):
    """Define el estado de una solicitud de retiro."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    COMPLETED = "completed"
    
class GroupMember(Base):
    """
    Modelo SQLAlchemy que representa la tabla 'group_members'.
    Establece la relación entre un usuario y un grupo al que pertenece.
    """
    __tablename__ = "group_members"

    
    
    group_id = Column(Integer, ForeignKey("groups.id"), primary_key=True)
    user_id = Column(Integer, primary_key=True, index=True)
    

    role = Column(SQLEnum(GroupRole), nullable=False, default=GroupRole.MEMBER)

    status = Column(SQLEnum(GroupMemberStatus), nullable=False, default=GroupMemberStatus.PENDING)

    internal_balance = Column(Numeric(10, 2), nullable=False, default=Decimal('0.00'))

    group = relationship("Group", back_populates="members")


# ... (después de la clase GroupMember) ...

class WithdrawalRequest(Base):
    """
    Modelo SQLAlchemy que representa la tabla 'withdrawal_requests'.
    Almacena las solicitudes de los miembros para retirar dinero del grupo.
    """
    __tablename__ = "withdrawal_requests"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False)
    member_user_id = Column(Integer, nullable=False) # El ID del miembro que solicita

    amount = Column(Numeric(10, 2), nullable=False)
    reason = Column(String(255), nullable=True) # Razón/descripción (ej. "para la cena")

    status = Column(SQLEnum(WithdrawalRequestStatus), nullable=False, default=WithdrawalRequestStatus.PENDING)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relación (opcional, pero buena práctica)
    group = relationship("Group")