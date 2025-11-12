import os
from dotenv import load_dotenv
from data_pipeline import REPO_ROOT

env_path = os.path.join(REPO_ROOT, ".env")

load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "etldb")

DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"