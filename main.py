import os
import pandas as pd
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


def main():
    start_time = time.time()
    # Extract
    csv_path = os.path.join('Data', 'Online Retail.csv')
    df = load_data(csv_path)

    # Timing the run
    end_time = time.time()
    total_time = end_time - start_time
    logging.info(f"ETL process completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
