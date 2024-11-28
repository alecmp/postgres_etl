from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

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
