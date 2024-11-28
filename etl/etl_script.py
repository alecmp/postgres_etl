from config import DATABASE_URL
from extract import extract_sales_data
from transform import transform_sales_data, prepare_tables
from load import insert_with_conflict_handling
from sqlalchemy import create_engine
from utils import log_info, log_error

def run_etl():
    try:
        # Estrazione dei dati
        api_url = "http://localhost:5000/sales"
        sales_data = extract_sales_data(api_url)
        log_info("Dati estratti con successo.")

        # Trasformazione dei dati
        df_sales = transform_sales_data(sales_data)
        df_products, df_stores, df_dates, df_facts = prepare_tables(df_sales)
        log_info("Dati trasformati con successo.")

        # Caricamento dei dati
        engine = create_engine(DATABASE_URL)

        insert_with_conflict_handling(df_products, 'products', engine, index_col='product_id')
        insert_with_conflict_handling(df_stores, 'stores', engine, index_col='store_id')
        insert_with_conflict_handling(df_dates, 'dates', engine, index_col='date_id')
        insert_with_conflict_handling(df_facts, 'sales_data', engine, index_col='sale_id')

        log_info("Dati caricati con successo in tutte le tabelle del database.")
        
    except Exception as e:
        log_error(f"Errore nel processo ETL: {e}")

if __name__ == "__main__":
    run_etl()
