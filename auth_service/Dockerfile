FROM python:3.11-slim

WORKDIR /app

# Copia e instala dependencias primero para aprovechar caché
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código de la aplicación
COPY . .

# Expone el puerto interno del contenedor
EXPOSE 8000

# Comando para iniciar el servidor Uvicorn
# Usa main:app (archivo:variable), escucha en todas las interfaces, puerto 8000, con recarga automática
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]