from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv()

dsn_postgre = {
    'dbname': os.environ.get("dbname"),
    'user': os.environ.get("user"),
    'password': os.environ.get("password"),
    'host': os.environ.get("host"),
    'port': os.environ.get("port"),
    'options': os.environ.get("options"),
}

path = Path(__file__).parent / "../03_sqlite_to_postgres/db.sqlite"
dsn_sqlite = {
    'path_to_db': path
}


