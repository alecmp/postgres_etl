# Importa la libreria dotenv per caricare le variabili di ambiente dal file .env
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Carica le variabili di ambiente dal file .env
load_dotenv()

# Recupera la variabile di ambiente DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL non trovata nel file .env")

# Connetti al database
engine = create_engine(DATABASE_URL)

# Crea la base per la definizione delle classi
Base = declarative_base()

# Definisci le classi per le tabelle del Data Warehouse
class Product(Base):
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True)
    product_name = Column(String, nullable=False)
    category = Column(String)
    price = Column(Float)

class Store(Base):
    __tablename__ = 'stores'
    store_id = Column(Integer, primary_key=True)
    store_name = Column(String, nullable=False)
    location = Column(String)

class DateDim(Base):
    __tablename__ = 'dates'
    date_id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    year = Column(Integer)
    quarter = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)

class SalesFact(Base):
    __tablename__ = 'sales_data'
    sale_id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    store_id = Column(Integer)
    date_id = Column(Integer)
    quantity_sold = Column(Integer)
    total_sales = Column(Float)

# Creare tutte le tabelle nel database
Base.metadata.create_all(engine)

