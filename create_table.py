# Importa la libreria dotenv per caricare le variabili di ambiente dal file .env
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

# Carica le variabili di ambiente dal file .env
load_dotenv()

# Recupera la variabile di ambiente DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL is None:
    raise ValueError("DATABASE_URL non trovata nel file .env")

# Connetti al database
engine = create_engine(DATABASE_URL)

# Creazione della tabella
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS sales_data (
            id SERIAL PRIMARY KEY,
            product_name VARCHAR(50),
            quantity INT,
            sale_date DATE DEFAULT CURRENT_DATE
        );
    """))
    conn.commit()  # Assicurati di usare commit se necessario
