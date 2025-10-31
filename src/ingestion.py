import pandas as pd
import requests
import os

class DataIngestion:
    def __init__(self, config):
        self.api_url = config['api_url']
        self.sales_path = config['sales_csv']
        self.inventory_path = config['inventory_csv']

    def run(self):
        os.makedirs('data/raw', exist_ok=True)

        response = requests.get(self.api_url)
        products = response.json()
        products_df = pd.DataFrame(products)
        products_df.to_parquet('data/raw/products.parquet', index=False)
        
        sales_df = pd.read_csv(self.sales_path)
        inventory_df = pd.read_csv(self.inventory_path)
        sales_df.to_parquet('data/raw/sales.parquet', index=False)
        inventory_df.to_parquet('data/raw/inventory.parquet', index=False)

        return products_df, sales_df, inventory_df
