import pandas as pd

class DataTransformation:
    def __init__(self, config):
        self.critical_stock_threshold = config['critical_stock_threshold']

    def run(self, products_df, sales_df, inventory_df):
        merged = pd.merge(products_df, inventory_df, on='id', how='left')
        
        merged = pd.merge(merged, sales_df, on='id', how='left')
        
        merged['critical_stock'] = merged['stock'] < merged['min_stock']
        
        merged['total_sales'] = merged.groupby('category')['quantity'].transform('sum')
        
        merged['profit'] = (merged['price'] - merged['cost']) * merged['quantity']
        
        critical_products = merged[merged['critical_stock'] == True][['id', 'title', 'stock', 'min_stock']]
        
        sales_by_category = merged.groupby('category')['quantity'].sum().to_dict()
        
        top_products = merged.groupby('title')['quantity'].sum().nlargest(5).to_dict()
        
        profit_by_product = merged.groupby('title')['profit'].sum().to_dict()
        
        metrics = {
            'critical_products': critical_products.to_dict(orient='records'),
            'sales_by_category': sales_by_category,
            'top_products': top_products,
            'profit_by_product': profit_by_product
        }
        merged.to_parquet('data/processed/merged_data.parquet', index=False)
        return metrics
