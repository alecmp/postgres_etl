import pandas as pd
from datetime import datetime

def transform_sales_data(data):
    """
    Trasforma i dati delle vendite in un DataFrame pandas e aggiunge le colonne necessarie.
    """
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

    return df_sales

def prepare_tables(df_sales):
    """
    Prepara i DataFrame separati per ciascuna tabella del database.
    """
    df_products = df_sales[['product_id', 'product_name', 'category', 'price']].drop_duplicates()
    df_stores = df_sales[['store_id', 'store_name', 'location']].drop_duplicates()
    df_dates = df_sales[['date_id', 'date']].drop_duplicates()
    df_dates['year'] = pd.to_datetime(df_dates['date']).dt.year
    df_dates['quarter'] = pd.to_datetime(df_dates['date']).dt.quarter
    df_dates['month'] = pd.to_datetime(df_dates['date']).dt.month
    df_dates['day'] = pd.to_datetime(df_dates['date']).dt.day
    df_facts = df_sales[['sale_id', 'product_id', 'store_id', 'date_id', 'quantity', 'total_sales']].rename(
        columns={'quantity': 'quantity_sold'}
    )

    return df_products, df_stores, df_dates, df_facts
