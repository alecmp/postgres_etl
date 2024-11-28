from flask import Flask, jsonify
from utils.data_generator import generate_sales_data  # Importa la funzione dal file data_generator

app = Flask(__name__)

# Usa la funzione generate_sales_data per ottenere dati casuali
@app.route('/sales', methods=['GET'])
def get_sales():
    sales_data = generate_sales_data(num_records=10)  # Genera 10 record di vendite
    return jsonify(sales_data)  # Restituisci i dati come JSON

if __name__ == '__main__':
    app.run(debug=True)
