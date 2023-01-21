#test2.py
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
#AWIC: Average weekly initial claims for unemployment insurance
#CGSORD: Manufacturers' new orders for consumer goods and materials
#VENDPERF: Vendor performance (the speed at which firms are delivering goods to customers)
#PERMIT: Building permits for new privately owned housing
#SP500: Stock prices (S&P 500 index)
#T10YFF: Interest rate spread (10-year Treasury bonds minus federal funds rate)
#UMCSENT: Index of consumer expectations
#M2SL: Money supply (M2)
#UMCSLDSV: Index of supplier deliveries (vendor performance)
#CGOORDD: Manufacturers' new orders, nondefense capital goods excluding aircraft
#UMFOOD: Manufacturers' unfilled orders
codes = ["IOER", "NASDAQ100", "DGS3MO", "DGS1MO", "DGS6MO", "DGS5", "DGS10",
         "AWHMAN", "PERMIT", "SP500", "T10YFF",
         "UMCSENT", "M2SL"]

to_find_codes = ["AWIC", "CGSORD","VENDPERF", "UMCSLDSV", "CGOORDD",  "UMFOOD"]

# Iterate through codes and retrieve data from FRED
for code in codes:
    # Retrieve data using pandas_datareader
    df = pdr.get_data_fred(code, start=two_years_ago, end=current_date)

    # Create table with the same name as code in the database
    df.to_sql(code, engine, if_exists='replace')