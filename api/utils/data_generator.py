from faker import Faker
import random

fake = Faker()

def generate_sales_data(num_records=10):
    sales_data = []
    for _ in range(num_records):
        sale = {
            "sale_id": fake.random_number(),
            "product_name": fake.word(),
            "quantity": random.randint(1, 200),
            "total_sales": random.uniform(10.0, 5000.0)
        }
        sales_data.append(sale)
    return sales_data
