# Define la imagen Docker para el servicio Watchdog.

# Imagen base ligera de Python
FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Instala las dependencias necesarias para watchdog.py (docker SDK y requests)
RUN pip install --no-cache-dir docker requests python-dotenv # Añadido python-dotenv si watchdog.py lo usa

# Copia el script del watchdog al directorio de trabajo
COPY watchdog.py .

# Comando para ejecutar el script watchdog.py al iniciar el contenedor.
# La bandera '-u' fuerza a que la salida (prints/logs) sea sin buffer,
# apareciendo inmediatamente en los logs de Docker.
CMD ["python", "-u", "watchdog.py"]