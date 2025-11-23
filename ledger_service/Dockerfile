# Define la imagen Docker para el Ledger Service.

# Imagen base ligera de Python
FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia e instala dependencias primero para optimizar el caché de Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código fuente de la aplicación
COPY . .

#Se copia el archivo de configuración seguro necesario para la conexión a la base de datos Cassandra en astra
COPY secure-connect-pixel-money-ledger.zip /app/secure-connect-pixel-money-ledger.zip

# Expone el puerto interno en el que escuchará el servidor Uvicorn
EXPOSE 8000

# Comando para iniciar el servidor Uvicorn
# Usa main:app (archivo:variable), escucha en 0.0.0.0, puerto 8000, con recarga automática
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]