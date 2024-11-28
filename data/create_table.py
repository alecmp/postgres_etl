# Importa la libreria dotenv per caricare le variabili di ambiente dal file .env
from db.models import Base, Product, Store, DateDim, SalesFact  # Importa Base e le classi dei modelli
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Carica le variabili di ambiente dal file .env
load_dotenv()

# Recupera la variabile di ambiente DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL non trovata nel file .env")

# Crea una connessione al database
engine = create_engine(DATABASE_URL)

# Creare tutte le tabelle nel database
Base.metadata.create_all(engine)

print("Tabelle create nel database con successo.")

