import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl')))

from config import DATABASE_URL
from sqlalchemy import create_engine, text

# Crea una connessione al database
engine = create_engine(DATABASE_URL)

def cleanup_database():
    """
    Svuota tutte le tabelle nel database per prepararlo al caricamento dei nuovi dati.
    """
    with engine.connect() as connection:
        # Elenco delle tabelle da svuotare
        tables = ['products', 'stores', 'dates', 'sales_data']
        
        # Ciclo su ciascuna tabella per svuotarla
        for table in tables:
            try:
                # Esegui il TRUNCATE per svuotare la tabella
                connection.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"))
                # Commit per rendere permanente la modifica
                connection.commit()
                print(f"Tabelle {table} svuotata con successo.")
            except Exception as e:
                print(f"Errore durante il svuotamento della tabella {table}: {e}")

if __name__ == "__main__":
    cleanup_database()
