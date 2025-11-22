# ledger_service/cassandra_db.py (Versión Limpia y Corregida)

import os
import logging
import time
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lee la configuración de Cassandra desde las variables de entorno
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "cassandra1").split(',')
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
CASSANDRA_USER = os.getenv("CASSANDRA_USER")
CASSANDRA_PASS = os.getenv("CASSANDRA_PASS")
CASSANDRA_DATACENTER = os.getenv("CASSANDRA_DATACENTER", "dc1")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wallet_ledger")

def get_cassandra_session() -> Session:
    """
    Establece y devuelve una conexión (sesión) con el cluster de Cassandra.
    Reintenta la conexión varias veces antes de fallar.
    """
    auth_provider = None
    if CASSANDRA_USER and CASSANDRA_PASS:
        auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)

    cluster = Cluster(
        contact_points=CASSANDRA_HOSTS,
        port=CASSANDRA_PORT,
        auth_provider=auth_provider,
        protocol_version=4
    )

    session = None
    attempts = 0
    max_attempts = 30
    retry_delay = 5 # segundos

    while attempts < max_attempts:
        attempts += 1
        try:
            logger.info(f"Intentando conectar a Cassandra (Intento {attempts}/{max_attempts})...")
            session = cluster.connect()
            logger.info("Conexión a Cassandra establecida exitosamente.")
            return session
        except Exception as e:
            logger.warning(f"Fallo al conectar a Cassandra: {e}. Reintentando en {retry_delay}s...")
            time.sleep(retry_delay)

    logger.error(f"Error fatal: No se pudo conectar a Cassandra después de {max_attempts} intentos.")
    return None

def create_keyspace_and_tables(session: Session):
    """
    Crea el Keyspace y las tablas necesarias si no existen.
    Esta función debe ser idempotente.
    """
    if not session:
        logger.error("No hay sesión de Cassandra para crear el schema.")
        return

    try:
        # --- 1. Crear Keyspace ---
        logger.info(f"Verificando/Creando keyspace '{KEYSPACE}'...")
        session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH REPLICATION = {{
            'class' : 'SimpleStrategy',
            'replication_factor' : 1
        }}
        """)

        # Seleccionar el keyspace para las siguientes operaciones
        session.set_keyspace(KEYSPACE)

        # --- 2. Crear Tabla 'transactions' (Búsqueda por ID) ---
        logger.info("Verificando/Creando tabla 'transactions'...")
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.transactions (
            id uuid PRIMARY KEY,
            user_id int,
            group_id int,
            source_wallet_type text,
            source_wallet_id text,
            destination_wallet_type text,
            destination_wallet_id text,
            type text,
            amount decimal,
            currency text,
            status text,
            metadata text,
            created_at timestamp,
            updated_at timestamp
        );
        """)

        # --- 3. Crear Tabla 'idempotency_keys' (Evitar duplicados) ---
        logger.info("Verificando/Creando tabla 'idempotency_keys'...")
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.idempotency_keys (
            key uuid PRIMARY KEY,
            transaction_id uuid
        );
        """)

        # --- 4. Crear Tabla 'transactions_by_user' (Historial de Usuario) ---
        logger.info("Verificando/Creando tabla 'transactions_by_user'...")
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.transactions_by_user (
            user_id int,
            created_at timestamp,
            id uuid,
            group_id int,
            source_wallet_type text,
            source_wallet_id text,
            destination_wallet_type text,
            destination_wallet_id text,
            type text,
            amount decimal,
            currency text,
            status text,
            metadata text,
            updated_at timestamp,
            PRIMARY KEY (user_id, created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC);
        """)

        # --- 5. Crear Tabla 'transactions_by_group' (Historial de Grupo) ---
        # ¡ESTA ES LA TABLA QUE FALLABA!
        logger.info("Verificando/Creando tabla 'transactions_by_group'...")
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.transactions_by_group (
            group_id int,
            created_at timestamp,
            id uuid,
            user_id int,
            source_wallet_type text,
            source_wallet_id text,
            destination_wallet_type text,
            destination_wallet_id text,
            type text,
            amount decimal,
            currency text,
            status text,
            metadata text,
            updated_at timestamp,
            PRIMARY KEY (group_id, created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC);
        """)

        # --- 6. Crear Índices (Si son necesarios) ---
        # (El índice en 'transactions' (user_id) no es ideal, pero lo dejamos por si acaso)
        logger.info("Verificando/Creando índices...")
        session.execute(f"""
        CREATE INDEX IF NOT EXISTS ON {KEYSPACE}.transactions (user_id);
        """)

        logger.info("Schema de Cassandra verificado/creado exitosamente.")

    except Exception as e:
        logger.error(f"Error fatal al crear/verificar el schema de Cassandra: {e}", exc_info=True)
        raise e # Relanzamos la excepción