# clean_data.py
from sqlalchemy import create_engine, inspect
import pandas as pd
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
    
    for table_name in table_names:
        df = pd.read_sql_table(table_name, engine)
        
        # Filter out missing values
        df = df.dropna()
        
        # Calculate moving average of second column
        moving_average = df.iloc[:, 1].rolling(window=10).mean()
        
        # Replace missing values in second column with moving average
        df.iloc[:, 1] = df.iloc[:, 1].fillna(moving_average)
        
        # Get IQR for moving average of second column
        Q1 = moving_average.quantile(0.05)
        Q3 = moving_average.quantile(0.95)
        IQR = Q3 - Q1
        
        # Identify and replace outliers in second column with moving average
        outliers = df.iloc[:, 1][((df.iloc[:, 1] < (Q1 - 1.5 * IQR)) | (df.iloc[:, 1] > (Q3 + 1.5 * IQR)))]
        df.iloc[outliers.index, 1] = moving_average[outliers.index]
        
        # Save cleaned data to table
        df.to_sql(table_name, engine, if_exists='replace')


if __name__ == "__main__":
    clean_data()
