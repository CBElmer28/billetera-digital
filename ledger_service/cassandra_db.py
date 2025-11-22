import os
import logging
import time
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory

# Configura logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Variables de Entorno
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "ledger")

# --- CORRECCIÓN AQUÍ ---
# 1. Buscamos la variable singular del .env
# 2. Si no existe, buscamos la plural
# 3. Si ninguna existe, usamos "localhost" (mejor para pruebas locales que "cassandra")
_host_env = os.getenv("CASSANDRA_HOST") or os.getenv("CASSANDRA_HOSTS") or "localhost"
CASSANDRA_HOSTS = _host_env.split(',')

CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", 9042))
CASSANDRA_USER = os.getenv("CASSANDRA_USER")
CASSANDRA_PASS = os.getenv("CASSANDRA_PASS")

# Variables para Astra DB
ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
ASTRA_DB_SECURE_BUNDLE_PATH = os.getenv("ASTRA_DB_SECURE_BUNDLE_PATH", "secure-connect-bundle.zip")

# Singleton de la sesión
cluster = None
session = None

def get_cassandra_session() -> Session:
    """
    Establece y devuelve una conexión (sesión) con Cassandra (Local o Astra).
    Nombre corregido para compatibilidad con main.py.
    """
    global cluster, session
    
    if session:
        return session

    attempts = 0
    max_attempts = 30
    retry_delay = 5

    while attempts < max_attempts:
        attempts += 1
        try:
            logger.info(f"Intentando conectar a Cassandra (Intento {attempts}/{max_attempts})...")
            
            # --- MODO 1: ASTRA DB (Nube) ---
            if ASTRA_DB_TOKEN and os.path.exists(ASTRA_DB_SECURE_BUNDLE_PATH):
                logger.info("Detectada configuración ASTRA DB. Conectando a la nube...")
                cloud_config = {
                    'secure_connect_bundle': ASTRA_DB_SECURE_BUNDLE_PATH
                }
                # En Astra, el 'username' siempre es 'token' y el password es tu token real
                auth_provider = PlainTextAuthProvider('token', ASTRA_DB_TOKEN)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            
            # --- MODO 2: CASSANDRA LOCAL (Docker) ---
            else:
                logger.info(f"Conectando a Cassandra Local en {CASSANDRA_HOSTS}:{CASSANDRA_PORT}...")
                auth_provider = None
                if CASSANDRA_USER and CASSANDRA_PASS:
                    auth_provider = PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASS)
                
                cluster = Cluster(
                    contact_points=CASSANDRA_HOSTS,
                    port=CASSANDRA_PORT,
                    auth_provider=auth_provider,
                    protocol_version=4
                )

            session = cluster.connect()
            
            # Configuración de la sesión
            session.row_factory = dict_factory 
            
            # Si es Local, intentamos crear el Keyspace (Astra no lo permite/necesita aquí)
            if not ASTRA_DB_TOKEN:
                try:
                    session.execute(f"""
                        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
                        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
                    """)
                except Exception as e:
                    logger.warning(f"No se pudo crear keyspace (normal si es Astra): {e}")

            session.set_keyspace(KEYSPACE)
            
            # Creamos las tablas aquí mismo para asegurar que existen antes de devolver la sesión
            create_keyspace_and_tables(session)
            
            logger.info("Conexión a Cassandra establecida y schema verificado.")
            return session

        except Exception as e:
            logger.warning(f"Fallo al conectar a Cassandra: {e}. Reintentando en {retry_delay}s...")
            time.sleep(retry_delay)

    raise Exception(f"Error fatal: No se pudo conectar a Cassandra después de {max_attempts} intentos.")

def create_keyspace_and_tables(session: Session):
    """
    Crea las tablas necesarias si no existen.
    Renombrado para compatibilidad con main.py.
    """
    logger.info("Verificando tablas...")

    # 1. Tabla Principal (Query por ID)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.transactions (
            id UUID PRIMARY KEY,
            user_id INT,
            group_id INT,
            source_wallet_type TEXT,
            source_wallet_id TEXT,
            destination_wallet_type TEXT,
            destination_wallet_id TEXT,
            type TEXT,
            amount DECIMAL,
            currency TEXT,
            status TEXT,
            metadata TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
    """)

    # 2. Idempotencia
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.idempotency_keys (
            key UUID PRIMARY KEY,
            transaction_id UUID
        )
    """)

    # 3. Historial por Usuario (Query por User + Fecha)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.transactions_by_user (
            user_id INT,
            created_at TIMESTAMP,
            id UUID,
            group_id INT,
            source_wallet_type TEXT,
            source_wallet_id TEXT,
            destination_wallet_type TEXT,
            destination_wallet_id TEXT,
            type TEXT,
            amount DECIMAL,
            currency TEXT,
            status TEXT,
            metadata TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY ((user_id), created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, id ASC)
    """)

    # 4. Historial por Grupo (Query por Group + Fecha)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.transactions_by_group (
            group_id INT,
            created_at TIMESTAMP,
            id UUID,
            user_id INT,
            source_wallet_type TEXT,
            source_wallet_id TEXT,
            destination_wallet_type TEXT,
            destination_wallet_id TEXT,
            type TEXT,
            amount DECIMAL,
            currency TEXT,
            status TEXT,
            metadata TEXT,
            updated_at TIMESTAMP,
            PRIMARY KEY ((group_id), created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, id ASC)
    """)
    
    logger.info("Tablas verificadas correctamente.")

# Función para inyección de dependencias (FastAPI)
def get_db():
    sess = get_cassandra_session()
    try:
        yield sess
    finally:
        pass