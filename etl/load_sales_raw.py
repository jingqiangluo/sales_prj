import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import psycopg2

# Show ALL columns
pd.set_option('display.max_columns', None)

# Show more rows (optional)
pd.set_option('display.max_rows', None)

# Increase width so columns don't get truncated
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)   # Full column content

# Optional: Nicer formatting
pd.set_option('display.float_format', '{:,.2f}'.format)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# extendable date formats list
date_formats = [
    '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d','%d-%m-%Y', '%d.%m.%Y', '%d-%b-%Y',
    '%b-%d-%Y', '%d-%b-%y', '%b-%d-%y', '%Y%m%d', '%m-%d-%Y', '%m/%d/%y'
]

# helper function 1 to clean up messy date formats
def parse_date(x):
    if pd.isna(x) or str(x).strip().lower() in ['nan', 'null', '', 'none', 'n/a']:        # edge cases, return pd.Nat
        return pd.NaT

    for fmt in date_formats:
        try:
            # Return full datetime if it is convertable
            dt = pd.to_datetime(x, format=fmt)
            return dt.date()
        except:
            continue

    logging.warning(f"Could not parse date: {x}")
    return pd.NaT

# helper function 2: to normalize discount to float
def normalize_discount(x):
    if pd.isna(x) or str(x).strip().lower() in ['nan', 'none', '']:  #edge cases, no discount
        return 0.0

    x = str(x).strip().replace('%', '').strip()   # Remove % sign and leading/trailing spaces

    try:
        val = float(x)

        # If it's a decimal proportion (0 to 1), convert to percentage; e.g. 0.75
        if 0 <= val <= 1:
            val = val * 100

        # Cap at reasonable values (negative value then 0.0 discount; over 100 then 100.0 discount)
        if val < 0:
            return 0.0
        if val > 100:
            return 100.0

        return round(val, 4)   # Keep up to 4 decimals if needed

    except:
        return 0.0

# helper function 3: to handle two specific customer types: Premier and Regular
def normalize_customer_type(x):
    if pd.isna(x):
        return np.nan

    x = str(x).strip().title()

    if 'Reg' in x:
        return 'Regular'
    elif 'Pre' in x:
        return 'Premier'

    return x

def clean_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardize the messy sales CSV file."""
    df_raw = df.copy()

    # Rename columns (remove spaces, put all lower cases, add '_', update % etc..)
    df_raw.columns = [col.strip().lower().replace(' ', '_').replace('%', '_pct') for col in df_raw.columns]

    # Date parsing and create a new column 'sale_date'
    df_raw['sale_date'] = df_raw['date'].apply(parse_date)
    df_raw = df_raw.dropna(subset=['sale_date'])

    # optional one line to clean mixed date formats
    # df_raw['sale_date'] = pd.to_datetime(df_raw['date'], format='mixed', errors='coerce', dayfirst=False)

    # Store ID normalization to number: remove all characters that are not numbers, then convert the data type to Int64
    df_raw['store_id'] = df_raw['store_id'].str.strip().str.upper().str.replace(r'[^0-9]', '', regex=True)
    df_raw['store_id'] = df_raw['store_id'].astype('Int64')

    # Product name cleaning: remove all white spaces
    df_raw['product_name'] = df_raw['product_name'].str.strip().str.replace(r'\s+', ' ', regex=True)

    # Quantity: set the column to numeric, set NaN to 0 and data type to int
    df_raw['quantity'] = pd.to_numeric(df_raw['quantity'], errors='coerce')
    df_raw['quantity'] = df_raw['quantity'].fillna(0).astype(int)

    # Price: remove '$' and leading/trailing spaces; then update the column to numeric
    df_raw['price'] = df_raw['price'].astype(str).str.replace(r'[\$,]', '', regex=True).str.strip()
    df_raw['price'] = pd.to_numeric(df_raw['price'], errors='coerce').fillna(0)

    # Discount: call helper function 2
    df_raw['discount_pct'] = df_raw['discount_pct'].apply(normalize_discount)

    # Customer type:  call helper function 3
    df_raw['customer_type'] = df_raw['customer_type'].apply(normalize_customer_type)

    # Payment method: format title() for all and fill none with 'Unknown'
    df_raw['payment_method'] = df_raw['payment_method'].fillna('Unknown').str.strip().str.title()

    # Transaction ID: format all to upper case and fill none with 'UNKNOWN'
    df_raw['transaction_id'] = df_raw['transaction_id'].fillna('UNKNOWN').str.strip().str.upper()

    # region: format all to be title()
    df_raw['region'] = df_raw['region'].str.strip().str.title()

    # 8. Derived columns (optional)
    df_raw['total_amount'] = df_raw['quantity'] * df_raw['price'] * (1 - df_raw['discount_pct'] / 100)
    df_raw['loaded_at'] = datetime.now(timezone.utc)

    # Final selection
    final_cols = [
        'transaction_id', 'sale_date', 'store_id', 'product_name', 'quantity',
        'price', 'discount_pct', 'total_amount', 'customer_type',
        'payment_method', 'region', 'loaded_at'
    ]
    return df_raw[final_cols].copy()


def load_to_postgres(df: pd.DataFrame):
    conn_str = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

    engine = create_engine(conn_str)

    try:
        with engine.connect() as conn:
            # Use text() for raw SQL
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))

            # Drop dependent objects first (important!)
            conn.execute(text("""
                            DROP VIEW IF EXISTS analytics.stg_sales CASCADE;
                            DROP TABLE IF EXISTS analytics.int_sales_daily CASCADE;
                            DROP TABLE IF EXISTS analytics.mart_sales_daily CASCADE;
                        """))

            # Optional: Commit (usually not needed, but safe)
            conn.commit()

        # Load the DataFrame
        df.to_sql(
            name='sales_raw',
            con=engine,
            schema='raw',
            if_exists='replace',  # change to 'append' later
            index=False,
            method='multi',
            chunksize=10000
        )

        logging.info(f"✅ Successfully loaded {len(df):,} rows into raw.sales_raw")

        # Validation
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM raw.sales_raw")).scalar()
            logging.info(f"Validation: {count} rows loaded successfully")

    except Exception as e:
        logging.error(f"❌ Load failed: {e}")
        raise


if __name__ == "__main__":
    # Load CSV
    csv_path = '/home/workdir/data/example_sales_data.csv'  # mount it into container
    df_raw = pd.read_csv(csv_path)
    logging.info(f"Loaded raw CSV with {len(df_raw)} rows")

    df_clean = clean_sales_data(df_raw)
    #print(df_clean)
    load_to_postgres(df_clean)
