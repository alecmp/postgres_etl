import os
from dotenv import load_dotenv

# Carica variabili di ambiente
load_dotenv()

# Recupera URL del database
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL non trovata nel file .env")
