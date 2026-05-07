import pytest
import pandas as pd
from load_sales_raw import clean_sales_data  # Make sure this import matches your file


def test_clean_sales_data():
    # Sample messy raw data
    sample_data = {
        'date': ['2023-01-15', '1/16/2023', None],
        'store ID': ['Store_005', 'STORE 005 ', 'storeID_005'],
        'PRODUCT_NAME': ['Laptop Ultra', 'Wireless Mouse ', None],
        ' quantity': [2, None, 5],
        'Price': ['$1299.99', '1499.99', '79.99'],
        ' discount%': ['0%', '10', None],
        'region': ['West', ' west ', 'WEST'],
        'customer_type': [None, 'premium', 'RegularCustomer']
    }

    df_raw = pd.DataFrame(sample_data)
    df_clean = clean_sales_data(df_raw)

    # Basic assertions
    assert len(df_clean) > 0
    assert 'sale_date' in df_clean.columns
    assert 'store_id' in df_clean.columns
    assert df_clean['quantity'].min() >= 0
    assert df_clean['price'].min() >= 0
    assert df_clean['transaction_id'].nunique() == len(df_clean)  # Should be unique now

    # Check cleaning worked
    assert df_clean['region'].str.contains('West').all()
    assert df_clean['customer_type'].iloc[0] == 'Unknown'


if __name__ == "__main__":
    pytest.main(["-v", "test_etl.py"])