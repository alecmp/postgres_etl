import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
import os
from datetime import datetime  

# Carica le variabili di ambiente dal file .env
load_dotenv()

# Recupera la variabile DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL non trovata nel file .env")

# Crea una connessione al database
engine = create_engine(DATABASE_URL)

# Funzione per gestire l'inserimento con `ON CONFLICT` evitando errori sui duplicati
def insert_with_conflict_handling(df, table_name, engine, index_col=None):
    """
    Inserisce un DataFrame nel database gestendo i duplicati.
    """
    if index_col:
        # Rimuovi duplicati basandoti sull'indice prima dell'inserimento
        df = df.drop_duplicates(subset=index_col)
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
    except IntegrityError as e:
        print(f"Errore durante l'inserimento in {table_name}: {e}")
    else:
        print(f"Dati caricati con successo nella tabella {table_name}.")

# Recupera i dati dall'API
response = requests.get("http://localhost:5000/sales")
data = response.json()
# Trasforma i dati in un DataFrame di pandas
df_sales = pd.DataFrame(data)

# Aggiungere colonne mancanti per le tabelle richieste
df_sales['product_id'] = df_sales['sale_id']  # Usa sale_id come identificativo temporaneo
df_sales['category'] = "miscellaneous"  # Categoria placeholder
df_sales['price'] = df_sales['total_sales'] / df_sales['quantity']  # Prezzo medio

df_sales['store_id'] = 1  # ID negozio placeholder
df_sales['store_name'] = "Main Store"  # Nome negozio placeholder
df_sales['location'] = "Default City"  # Posizione negozio placeholder

df_sales['date_id'] = df_sales['sale_id']  # ID data placeholder
df_sales['date'] = [datetime.now().strftime("%Y-%m-%d") for _ in range(len(df_sales))]

# Tabella products
df_products = df_sales[['product_id', 'product_name', 'category', 'price']].drop_duplicates()

# Tabella stores
df_stores = df_sales[['store_id', 'store_name', 'location']].drop_duplicates()

# Tabella dates
df_dates = df_sales[['date_id', 'date']].drop_duplicates()
df_dates['year'] = pd.to_datetime(df_dates['date']).dt.year
df_dates['quarter'] = pd.to_datetime(df_dates['date']).dt.quarter
df_dates['month'] = pd.to_datetime(df_dates['date']).dt.month
df_dates['day'] = pd.to_datetime(df_dates['date']).dt.day

# Tabella sales_data
df_facts = df_sales[['sale_id', 'product_id', 'store_id', 'date_id', 'quantity', 'total_sales']].rename(
    columns={'quantity': 'quantity_sold'}
)

# Carica i dati nel database
insert_with_conflict_handling(df_products, 'products', engine, index_col='product_id')
insert_with_conflict_handling(df_stores, 'stores', engine, index_col='store_id')
insert_with_conflict_handling(df_dates, 'dates', engine, index_col='date_id')
insert_with_conflict_handling(df_facts, 'sales_data', engine, index_col='sale_id')

print("Dati caricati con successo in tutte le tabelle del database.")