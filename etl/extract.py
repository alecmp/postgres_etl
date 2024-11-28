import requests

def extract_sales_data(api_url):
    """
    Estrae i dati delle vendite dall'API.
    """
    response = requests.get(api_url)
    data = response.json()
    return data
