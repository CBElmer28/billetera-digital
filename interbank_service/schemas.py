"""Modelos Pydantic (schemas) para la API del Interbank Service (simulador)."""

from pydantic import BaseModel, Field
from typing import Optional

class InterbankTransferRequest(BaseModel):
    """
    Schema para validar el cuerpo (body) JSON de una solicitud de transferencia
    entrante desde otro banco (ej. Pixel Money).
    """
    origin_bank: str = Field(..., description="Nombre del banco que envía (ej. PIXEL_MONEY)")
    origin_account_id: Optional[str] = Field(None, description="ID de cuenta/usuario en el banco origen (opcional)")
    destination_bank: str = Field(..., description="Nombre del banco destino (debería ser HAPPY_MONEY para este servicio)")
    destination_phone_number: str = Field(..., min_length=9, max_length=15, description="Número de celular del destinatario en este banco")
    amount: float = Field(..., gt=0, description="Monto a transferir (debe ser positivo)")
    currency: str = Field(..., description="Código de moneda (ej. PEN, USD)")
    transaction_id: str = Field(..., description="ID único de la transacción generado por el banco origen")
    description: Optional[str] = Field(None, max_length=255, description="Descripción opcional de la transferencia")