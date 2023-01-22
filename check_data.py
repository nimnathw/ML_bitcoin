# check_data.py
from sqlalchemy import create_engine, inspect
import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()

# Connect to the database
user=os.environ.get("MYSQL_USER")
password=os.environ.get("MYSQL_PASSWORD")
host=os.environ.get("MYSQL_HOST")
database=os.environ.get("MYSQL_DATABASE")

    
def clean_data():

    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    #list to hold all dataframes
    dfs = []
    for table_name in table_names:
        df = pd.read_sql_table(table_name, engine)
        dfs.append(df)
        missing_values = df.isnull().sum()
        # Perform data exploration
        print(df.info)
        #print(df.describe)
        print(f"No. missing values in table {table_name} is: {missing_values} \n")
        print(df.iloc[:, 1].head(5))

        # Filter out missing values
        df = df.dropna()

        # Get IQR for numeric column
        moving_average = df.iloc[:, 1].rolling(window=10).mean()
        # Get IQR for moving average of second column
        Q1 = moving_average.quantile(0.05)
        Q3 = moving_average.quantile(0.95)
        IQR = Q3 - Q1

        # Count number of observations out of 1.5* IQR
        outliers = df.iloc[:, 1][((df.iloc[:, 1] < (Q1 - 1.5 * IQR)) | (df.iloc[:, 1] > (Q3 + 1.5 * IQR)))]
        num_outliers = outliers.shape[0]

        # Count number of observations out of 1.5* IQR
        num_outliers = outliers.shape[0]

        print(f"No. outlier values in table {table_name} is: {num_outliers} \n")

        

if __name__ == "__main__":
    clean_data()
