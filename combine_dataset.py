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

def combine_data():
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    dfs = []
    cut_off_date = '2022-11-01'

    for table_name in table_names:
        df = pd.read_sql_table(table_name, engine)

        # Rename second column to "date" and convert it to datetime
        df = df.rename(columns={df.columns[1]: "date"})
        df["date"] = pd.to_datetime(df["date"])
        
        # Set "date" column as the index
        df = df.set_index('date')

        # Drop the column named "index"
        df.drop(columns=['index'], inplace=True)
        
        # Determine the frequency of the data and resample accordingly
        if df.shape[0] < 30:
            df = df.resample('D').ffill()
        elif df.shape[0] < 120:
            df = df.resample('D').ffill()

        # Remove rows with date <= cut_off_date
        df = df[df.index <= cut_off_date]

        # Add the dataframe to the list
        dfs.append(df)

    # Use the pd.merge() function to merge all dataframes in the list on the index
    merged_df = dfs[0]
    for i in range(1, len(dfs)):
        merged_df = pd.merge(merged_df, dfs[i], left_index=True, right_index=True, how='outer')

    # Remove rows with at least one NaN value
    merged_df = merged_df.dropna()
    
    print(merged_df.head())
    merged_df.to_csv('data.csv', date_format='%Y-%m-%d', index=True)

        
if __name__ == "__main__":
    combine_data()
