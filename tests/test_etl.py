import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from db.models import Base, Product, Store, DateDim, SalesFact
from dotenv import load_dotenv
import os

# Carica le variabili di ambiente dal file .env
load_dotenv()

# Ottieni la URL del database di test
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

if TEST_DATABASE_URL is None:
    raise ValueError("TEST_DATABASE_URL non trovata nel file .env")

# Crea una connessione al database di test
engine = create_engine(TEST_DATABASE_URL)

# Funzione di setup per creare le tabelle nel database di test
@pytest.fixture(scope="module")
def setup_database():
    # Crea tutte le tabelle nel database di test
    Base.metadata.create_all(engine)
    yield  # Esegui i test dopo
    # Dopo i test, svuota tutte le tabelle
    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE products, stores, dates, sales_data RESTART IDENTITY CASCADE;")

# Test per la connessione al database di test
def test_db_connection():
    with engine.connect() as conn:
        result = conn.execute("SELECT 1")
        assert result.scalar() == 1

# Test per verificare che le tabelle siano state create
def test_tables_created(setup_database):
    # Verifica che le tabelle esistano
    inspector = engine.dialect.get_inspector(engine)
    tables = inspector.get_table_names()
    assert 'products' in tables
    assert 'stores' in tables
    assert 'dates' in tables
    assert 'sales_data' in tables

# Test per il processo ETL (estrazione, trasformazione e caricamento dei dati)
def test_etl_process(setup_database):
    # Testa l'estrazione dei dati
    response = requests.get("http://localhost:5000/sales")
    data = response.json()
    assert len(data) > 0  # Verifica che ci siano dati

    # Testa la trasformazione (creazione dei DataFrame)
    df_sales = pd.DataFrame(data)
    df_sales['product_id'] = df_sales['sale_id']  # Usa sale_id come identificativo temporaneo
    df_sales['category'] = "miscellaneous"  # Categoria placeholder
    df_sales['price'] = df_sales['total_sales'] / df_sales['quantity']  # Prezzo medio

    df_sales['store_id'] = 1  # ID negozio placeholder
    df_sales['store_name'] = "Main Store"  # Nome negozio placeholder
    df_sales['location'] = "Default City"  # Posizione negozio placeholder

    df_sales['date_id'] = df_sales['sale_id']  # ID data placeholder
    df_sales['date'] = [datetime.now().strftime("%Y-%m-%d") for _ in range(len(df_sales))]

    # Testa che il DataFrame sia correttamente trasformato
    assert 'product_id' in df_sales.columns
    assert 'category' in df_sales.columns
    assert 'price' in df_sales.columns
    assert len(df_sales) > 0

    # Testa il caricamento dei dati nel database
    insert_with_conflict_handling(df_sales[['product_id', 'product_name', 'category', 'price']].drop_duplicates(), 'products', engine, index_col='product_id')
    insert_with_conflict_handling(df_sales[['store_id', 'store_name', 'location']].drop_duplicates(), 'stores', engine, index_col='store_id')
    insert_with_conflict_handling(df_sales[['date_id', 'date']].drop_duplicates(), 'dates', engine, index_col='date_id')

    # Verifica che i dati siano stati caricati nel database
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM products")
        assert result.scalar() > 0

        result = conn.execute("SELECT COUNT(*) FROM stores")
        assert result.scalar() > 0

        result = conn.execute("SELECT COUNT(*) FROM dates")
        assert result.scalar() > 0

# Test di integrità dei dati: verificare che i dati non abbiano duplicati
def test_data_integrity(setup_database):
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(DISTINCT product_id) FROM products")
        assert result.scalar() == 1  # Supponiamo che il test abbia inserito solo un prodotto, verifica l'unicità

    # Aggiungere ulteriori verifiche per le altre tabelle se necessario

