#retrieve_data.py

from sqlalchemy import create_engine
import requests
import json
from datetime import datetime, timedelta
import pandas as pd
import pandas_datareader as pdr
import os
from dotenv import load_dotenv
load_dotenv()

user=os.environ.get("MYSQL_USER")
password=os.environ.get("MYSQL_PASSWORD")
host=os.environ.get("MYSQL_HOST")
database=os.environ.get("MYSQL_DATABASE")

engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Get current date and two years ago
current_date = datetime.now()
two_years_ago = current_date - timedelta(days=365*2)

# Define list of codes to retrieve from FRED
#IOER: Interest on Excess Reserves of Depository Institutions
#NASDAQ100: NASDAQ 100 Stock Index
#DGS3MO: Treasury Constant Maturity Rate: 3 Month
#DGS1MO: Treasury Constant Maturity Rate: 1 Month
#DGS6MO: Treasury Constant Maturity Rate: 6 Month
#DGS5: Treasury Constant Maturity Rate: 5 Years
#DGS10: Treasury Constant Maturity Rate: 10 Years
#AWHMAN: Average weekly hours of manufacturing workers
#IC4WSA: 4-Week Moving Average of Initial Claims
#ACDGNO: Manufacturers' New Orders: Consumer Durable Goods
#RETAILIMSA: Retailers Inventories
#PERMIT: Building permits for new privately owned housing
#SP500: Stock prices (S&P 500 index)
#T10YFF: Interest rate spread (10-year Treasury bonds minus federal funds rate)
#UMCSENT: Index of consumer expectations
#M2SL: Money supply (M2)
#PCUADLVWRADLVWR: Producer Price Index by Industry: Delivery and Warehouse Industries
#NEWORDER:Manufacturers' New Orders: Nondefense Capital Goods Excluding Aircraft
#AMTUNO: Manufacturers' New Orders: Manufacturing with Unfilled Orders
fred_codes = ["IOER", "NASDAQ100", "DGS3MO", "DGS1MO", "DGS6MO", "DGS5", "DGS10",
         "AWHMAN", "PERMIT", "SP500", "T10YFF", "IC4WSA", "ACDGNO", "RETAILIMSA", 
         "UMCSENT", "M2SL", "PCUADLVWRADLVWR"]

coingecko_codes = ["prices", "market_caps", "total_volumes"]

def retrieve_fred(fred_codes, start, end):
    """
    Retrieve economic data from FRED and save it to a MySQL database.
    
    Parameters:
    - fred_codes (list): List of codes to retrieve data for
    - start_date (datetime): Start date for data retrieval
    - end_date (datetime): End date for data retrieval
    """
    
    # Connect to the database
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
    
    try:
        for code in fred_codes:
            df = pdr.get_data_fred(code, start=start, end=end)
            df.to_sql(code, engine, if_exists='replace')
        print(f'Successfully retrieved data for codes: {fred_codes}')
    except Exception as e:
        print(f'Error retrieving data for codes: {fred_codes}')
        print(e)

def retrieve_coingecko(coingecko_codes):
    """
    Retrieve Bitcoin data from coingecko API endpoint and save it to a MySQL database.
    
    Parameters:
    - coingecko codes (list): List of codes retrieved from API endpoint
    """
    # Connect to the database
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
    
    try:
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=730&interval=daily"
        response = requests.get(url)

        # Parse the response from the API
        data = json.loads(response.text)

        for code in coingecko_codes: 
            # Convert the JSON data to a pandas DataFrame
            df = pd.DataFrame(data[code], columns=["date", code])

            # Convert the 'date' column to datetime format
            df['date'] = pd.to_datetime(df['date'], unit='ms')

            # Create table with the same name as code in the database
            df.to_sql(code, engine, if_exists='replace')
    except Exception as e:
        print("Error occurred while retrieving data from CoinGecko: ", e)


# retrieve data from fred
retrieve_fred(fred_codes, two_years_ago, current_date)

# retrieve data from coingecko
retrieve_coingecko(coingecko_codes)