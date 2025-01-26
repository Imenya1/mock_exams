import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import logging
from io import StringIO

# logging
logging.basicConfig(filename='scraping_log.txt', level=logging.INFO, format='%(asctime)s - %(message)s')

# Database connection variable
URL = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
EURO_RATE = 0.93
POUND_RATE = 0.8
INR_RATE = 82.95
DATABASE_NAME = 'Largest_banks'
TABLE_NAME = 'Market_Capitalization'
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'password'
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = '5432'

try:
    # Scrape the data
    logging.info("Starting to scrape the data from the website.")
    response = requests.get(URL)
    response.raise_for_status()  # Raise exception for HTTP errors
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})
    
    if table is None:
        logging.error("Table not found on the webpage.")
        raise ValueError("Table not found on the webpage.")
    
    # Parse the table into a DataFrame
    df = pd.read_html(StringIO(str(table)))[0]
    
    if df.empty:
        logging.error("The DataFrame is empty after scraping.")
        raise ValueError("The DataFrame is empty after scraping.")
    
    # Check if columns are multi-level and drop the first level if necessary
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(0)

    # Print column names to debug
    logging.info(f"Columns in the DataFrame: {df.columns}")
    
    # Adjust column names and filter relevant data
    df = df[['Bank name', 'Market cap (US$ billion)']]  # Adjust column names as per actual scraped data
    df.rename(columns={'Bank name': 'Bank Name', 'Market cap (US$ billion)': 'Market Cap (USD Billion)'}, inplace=True)
    
    # Convert market capitalization to different currencies
    df['Market Cap (Euro Billion)'] = (df['Market Cap (USD Billion)'] * EURO_RATE).round(2)
    df['Market Cap (Pound Billion)'] = (df['Market Cap (USD Billion)'] * POUND_RATE).round(2)
    df['Market Cap (INR Billion)'] = (df['Market Cap (USD Billion)'] * INR_RATE).round(2)
    
    # Save to CSV
    csv_file = 'largest_banks.csv'
    df.to_csv(csv_file, index=False)
    logging.info(f"Data saved to CSV file: {csv_file}")

    # Connect to PostgreSQL and create database if it doesn't exist
    engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{DATABASE_NAME}')
    
    # Check if the database exists, create if not
    if not database_exists(engine.url):
        create_database(engine.url)
        logging.info(f"Database {DATABASE_NAME} created successfully.")
    else:
        logging.info(f"Database {DATABASE_NAME} already exists.")
    
    # Check if table exists and replace it
    df.to_sql(TABLE_NAME, con=engine, if_exists='replace', index=False)
    logging.info(f"Data loaded to the database table {TABLE_NAME}.")
    
    
    print(f"Data processed and saved to'{DATABASE_NAME}' database in PostgreSQL in table '{TABLE_NAME}'Successfully!")

except requests.exceptions.RequestException as e:
    logging.error(f"An error occurred while making the request: {e}")
    print(f"An error occurred while making the request: {e}")

except Exception as e:
    logging.error(f"An error occurred: {e}")
    print(f"An error occurred: {e}")