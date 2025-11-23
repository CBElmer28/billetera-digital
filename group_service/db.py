"""Configuración de la conexión a la base de datos MariaDB usando SQLAlchemy para el Group Service."""

import os
import logging
import time
from fastapi import HTTPException
from fastapi.exceptions import RequestValidationError
from sqlalchemy import create_engine, exc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carga variables de entorno desde el archivo .env
load_dotenv()

# Lee las credenciales de la base de datos desde el entorno
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

# Valida que las variables necesarias estén presentes
required_db_vars = {"DB_USER", "DB_PASS", "DB_HOST", "DB_NAME"}
missing_vars = required_db_vars - set(os.environ)
if missing_vars:
    logger.error(f"Faltan variables de entorno para la base de datos: {', '.join(missing_vars)}")
   

SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"



engine = None
attempts = 0
max_attempts = 30
wait_time = 10 # 10 segundos

while attempts < max_attempts and engine is None:
    try:
        attempts += 1
        logger.info(f"Intentando conectar a MariaDB (Intento {attempts}/{max_attempts})...")
        engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)

        # Intenta conectar para verificar credenciales Y que la BD exista
        with engine.connect() as connection:
            logger.info("✅ Conexión a la base de datos (MariaDB) establecida exitosamente.")

    except exc.SQLAlchemyError as e:
        logger.warning(f"Fallo al conectar a MariaDB: {e}")
        if attempts < max_attempts:
            time.sleep(wait_time)
        else:
            logger.error("No se pudo conectar a MariaDB después de %d intentos.", max_attempts)
            engine = None



# Crea una fábrica de sesiones (SessionLocal)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine) if engine else None

# Crea una clase base (Base) para los modelos declarativos
Base = declarative_base()


def get_db():
    if SessionLocal is None:
        logger.error("La fábrica de sesiones de base de datos no está inicializada.")
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    db = SessionLocal()
    try:
        yield db
    except RequestValidationError as validation_exc:
        logger.warning(f"Error de validación: {validation_exc.errors()}")
        db.rollback()
        raise validation_exc
    except HTTPException as http_exc:
        db.rollback()
        logger.warning(f"Error HTTP controlado: {http_exc.detail}")
        raise http_exc
    except exc.SQLAlchemyError as e:
        db.rollback()
        logger.error(f"Error de base de datos durante la petición: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error interno de base de datos.")
    except Exception as e:
         db.rollback() 
         logger.error(f"Error inesperado (no-HTTP) durante la petición: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail="Error interno del servidor.")
    finally:
        db.close()