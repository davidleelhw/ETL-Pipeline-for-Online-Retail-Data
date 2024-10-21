import os
import pandas as pd
import psycopg2
import logging
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection info from environment variables
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Set up logging
log_folder = 'logs'
if not os.path.exists(log_folder):
    os.makedirs(log_folder)
log_filename = os.path.join(log_folder, f'run_{time.strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)

def load_data(file_path):
    """
    Load CSV data into a pandas DataFrame.
    Parameters:
        file_path (str): The path to the CSV file to be loaded.
    Returns:
        DataFrame: A pandas DataFrame containing the loaded data.
    """
    try:
        start = time.time()
        df = pd.read_csv(file_path)
        logging.info(f"Data loaded successfully from CSV. DataFrame Shape: {df.shape}.")
        logging.info(f"Data loading took {time.time() - start:.2f} seconds.")
        return df
    except Exception as e:
        logging.error(f"Error loading CSV file: {e}")
        raise

def transform_data(df):
    """
    Transform data types and handle missing values.
    Parameters:
        df (DataFrame): A pandas DataFrame containing the untransformed data.
    Returns:
        DataFrame: A pandas DataFrame containing the transformed data.
    """
    try:
        # Drop rows where 'InvoiceNo' is missing
        df.dropna(subset=['InvoiceNo'], inplace=True)

        # Assuming -1 as placeholder for missing customer IDs
        df['CustomerID'] = df['CustomerID'].fillna(-1).astype(int)

        # Handle missing 'StockCode' and 'Description'
        df.dropna(subset=['StockCode', 'Description'], how='all', inplace=True)     # Drop rows where both 'StockCode' and 'Description' are missing
        mapping = df[['StockCode', 'Description']].dropna().drop_duplicates()       # Create a dataframe for mapping, removing missing and duplicate values
        description_fill = dict(zip(mapping['StockCode'], mapping['Description']))  # Create a mapping dictionary for filling 'Description'
        stockcode_fill = dict(zip(mapping['Description'], mapping['StockCode']))    # Create a mapping dictionary for filling 'StockCode'
        df['Description'] = df['Description'].fillna(df['StockCode'].map(description_fill)) # Handle missing 'Description' based on 'StockCode'
        df['StockCode'] = df['StockCode'].fillna(df['Description'].map(stockcode_fill))     # Handle missing 'StockCode' based on 'Description'

        # Fill missing 'Quantity' with 0
        df['Quantity'] = df['Quantity'].fillna(0).astype(int)

        # Fill missing 'UnitPrice' with 0
        df['UnitPrice'] = df['UnitPrice'].fillna(0).astype(float)

        # Handle missing 'InvoiceDate' based on 'InvoiceNo'
        # Create a Series with unique InvoiceNo and corresponding InvoiceDate
        invoice_date_fill = df[['InvoiceNo', 'InvoiceDate']].dropna().drop_duplicates().set_index('InvoiceNo')['InvoiceDate']

        # If 'InvoiceNo' is unique, map directly to fill missing InvoiceDate
        if invoice_date_fill.index.is_unique:
            df['InvoiceDate'] = df['InvoiceDate'].fillna(df['InvoiceNo'].map(invoice_date_fill))
        # If there are multiple 'InvoiceDate' values for one 'InvoiceNo', use the first occurrence of 'InvoiceDate'
        else:
            invoice_date_fill = invoice_date_fill.groupby(invoice_date_fill.index).first()
            df['InvoiceDate'] = df['InvoiceDate'].fillna(df['InvoiceNo'].map(invoice_date_fill))

        # Fill any remaining missing 'InvoiceDate' with default value
        df['InvoiceDate'] = df['InvoiceDate'].fillna('1/1/2010 00:00:00 AM')
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M')

        logging.info("Data transformation completed.")
        return df
    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        raise

def main():
    start_time = time.time()
    # Extract
    csv_path = os.path.join('Data', 'Online Retail.csv')
    df = load_data(csv_path)

    # Transform
    df = transform_data(df)
    df.to_csv("temp.csv")
    
    # Connect to DB and create tables

    # Load data into the database

    # Close connection

    # Timing the run
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"ETL process completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
