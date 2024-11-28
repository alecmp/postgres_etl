import os
from dotenv import load_dotenv

# Carica variabili di ambiente
load_dotenv()

# Recupera URL del database
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL is None:
    raise ValueError("DATABASE_URL non trovata nel file .env")

# Recupera il token di Product Hunt
PRODUCT_HUNT_API_KEY = os.getenv("PRODUCT_HUNT_API_KEY")
if PRODUCT_HUNT_API_KEY is None:
    raise ValueError("PRODUCT_HUNT_API_KEY non trovata nel file .env")
