import pandas as pd

class DataQuality:
    def run(self, products_df, sales_df, inventory_df):
        results = {}
        results['negative_prices'] = not (products_df['price'] < 0).any()
        results['valid_stock'] = inventory_df['stock'].apply(lambda x: isinstance(x, (int, float)) and x >= 0).all()
        results['valid_categories'] = products_df['category'].notnull().all()
        try:
            pd.to_datetime(sales_df['date'])
            results['valid_dates'] = True
        except Exception:
            results['valid_dates'] = False
        return results
