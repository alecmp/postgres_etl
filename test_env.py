from sqlalchemy import create_engine, text

# URL di connessione
DATABASE_URL = "postgresql://alessandro:ciaobelli@localhost:5432/dw_database"

# Crea il motore di connessione
engine = create_engine(DATABASE_URL)

# Esecuzione della query
with engine.connect() as conn:
    result = conn.execute(text("SELECT 'Hello, Data Warehouse!'"))
    print(result.fetchone())