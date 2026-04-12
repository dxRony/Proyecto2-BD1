import os
from dotenv import load_dotenv
from firebird.driver import connect, driver_config

load_dotenv()

def get_db_config():
    return {
        "host": os.getenv("FB_HOST", "localhost"),
        "database": os.getenv("FB_DATABASE", "/var/lib/firebird/data/indicadores_gt.fdb"),
        "user": os.getenv("FB_USER", "SYSDBA"),
        "password": os.getenv("FB_PASSWORD", "masterkey"),
        "charset": os.getenv("FB_CHARSET", "UTF8"),
    }

def get_connection():
    config = get_db_config()

    driver_config.server_defaults.host.value = config["host"]
    driver_config.server_defaults.user.value = config["user"]
    driver_config.server_defaults.password.value = config["password"]

    return connect(
        config["database"],
        charset=config["charset"]
    )

def test_connection():
    conn = None
    try:
        conn = get_connection()
        print("conectado a Firebird")
    except Exception as e:
        print("Error de coneccion:", e)
    finally:
        if conn:
            conn.close()