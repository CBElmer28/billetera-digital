# Define la imagen Docker para el Group Service.

# Imagen base ligera de Python
FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia e instala dependencias primero para optimizar el caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código fuente de la aplicación
COPY . .

# Expone el puerto interno en el que escuchará el servidor Uvicorn
EXPOSE 8000

# Comando para iniciar el servidor Uvicorn
# Usa main:app (archivo:variable), escucha en 0.0.0.0, puerto 8000, con recarga automática
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]