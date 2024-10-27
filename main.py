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

def connect_to_db():
    """Connect to PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("Connected to the database.")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

def create_tables(conn):
    """Create tables in PostgreSQL."""
    try:
        start = time.time()
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS onlineretail.customers (
                customerid INT PRIMARY KEY,
                country VARCHAR(100)
            );
            
            CREATE TABLE IF NOT EXISTS onlineretail.products (
                stockcode VARCHAR(20) PRIMARY KEY,
                description TEXT,
                unitprice NUMERIC
            );
            
            CREATE TABLE IF NOT EXISTS onlineretail.orders (
                invoiceno VARCHAR(20) PRIMARY KEY,
                customerid INT REFERENCES onlineretail.customers(customerid),
                invoicedate TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS onlineretail.orderdetails (
                orderdetailid INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                invoiceno VARCHAR(20) REFERENCES onlineretail.orders(invoiceno),
                stockcode VARCHAR(20) REFERENCES onlineretail.products(stockcode),
                quantity INT,
                unitprice NUMERIC
            );
            """)
            conn.commit()
            logging.info("Tables created successfully.")
            logging.info(f"Tables creation took {time.time() - start:.2f} seconds.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creating tables: {e}")
        raise

def extract_data(file_path):
    """
    Extract CSV data into a pandas DataFrame.
    Parameters:
        file_path (str): The path to the CSV file to be loaded.
    Returns:
        DataFrame: A pandas DataFrame containing the loaded data.
    """
    try:
        start = time.time()
        df = pd.read_csv(file_path)
        logging.info(f"Data extracted successfully from CSV. DataFrame Shape: {df.shape}.")
        logging.info(f"Data extracting took {time.time() - start:.2f} seconds.")
        return df
    except Exception as e:
        logging.error(f"Error extracting CSV file: {e}")
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

def load_data(conn, df):
    """Insert data into PostgreSQL with independent transactions for each table."""
    
    # Insert into customers
    try:
        start = time.time()
        with conn.cursor() as cur:
            customers = df[['CustomerID', 'Country']].drop_duplicates()
            for index, row in customers.iterrows():
                cur.execute("""
                    INSERT INTO onlineretail.customers (customerid, country)
                    VALUES (%s, %s)
                    ON CONFLICT (customerid) DO NOTHING;
                """, (row['CustomerID'], row['Country']))
            conn.commit()  # Commit after inserting customers
            logging.info(f"{len(customers)} customers inserted.")
            logging.info(f"Customers insertion took {time.time() - start:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error inserting customers: {e}")
    
    # Insert into products
    try:
        start = time.time()
        with conn.cursor() as cur:
            products = df[['StockCode', 'Description', 'UnitPrice']].drop_duplicates()
            for index, row in products.iterrows():
                cur.execute("""
                    INSERT INTO onlineretail.products (stockcode, description, unitprice)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (stockcode) DO NOTHING;
                """, (row['StockCode'], row['Description'], row['UnitPrice']))
            conn.commit()  # Commit after inserting products
            logging.info(f"{len(products)} products inserted.")
            logging.info(f"Products insertion took {time.time() - start:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error inserting products: {e}")
    
    # Insert into orders
    try:
        start = time.time()
        with conn.cursor() as cur:
            orders = df[['InvoiceNo', 'CustomerID', 'InvoiceDate']].drop_duplicates()
            for index, row in orders.iterrows():
                cur.execute("""
                    INSERT INTO onlineretail.orders (invoiceno, customerid, invoicedate)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (invoiceno) DO NOTHING;
                """, (row['InvoiceNo'], row['CustomerID'], row['InvoiceDate']))
            conn.commit()  # Commit after inserting orders
            logging.info(f"{len(orders)} orders inserted.")
            logging.info(f"Orders insertion took {time.time() - start:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error inserting orders: {e}")
    
    # Insert into orderdetails
    try:
        start = time.time()
        with conn.cursor() as cur:
            order_details = df[['InvoiceNo', 'StockCode', 'Quantity', 'UnitPrice']]
            for index, row in order_details.iterrows():
                cur.execute("""
                    INSERT INTO onlineretail.orderdetails (invoiceno, stockcode, quantity, unitprice)
                    VALUES (%s, %s, %s, %s);
                """, (row['InvoiceNo'], row['StockCode'], row['Quantity'], row['UnitPrice']))
            conn.commit()  # Commit after inserting orderdetails
            logging.info(f"{len(order_details)} order details inserted.")
            logging.info(f"Order details insertion took {time.time() - start:.2f} seconds.")
    except Exception as e:
        logging.error(f"Error inserting order details: {e}")

def main():
    start_time = time.time()
    # Extract
    csv_path = os.path.join('Data', 'Online Retail.csv')
    df = extract_data(csv_path)

    # Transform
    df = transform_data(df)
    df.to_csv("temp.csv")
    
    # Connect to DB and create tables
    conn = connect_to_db()
    create_tables(conn)

    # Load data into the database
    load_data(conn, df)

    # Close connection
    conn.close()
    logging.info("Database connection closed.")

    # Timing the run
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"ETL process completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
