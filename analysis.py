from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Load the cleaned data into a DataFrame
df = pd.read_csv("data.csv")
df['date'] = pd.to_datetime(df['date'])
df = df[df['date'] > '2022-01-01']
df.set_index('date', inplace=True)


# view data
print(df.describe())
