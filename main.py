import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Integer

engine = create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format("root", "", "localhost", 3306, "pandas_data"))
#engine = create_engine('mysql+pymysql://root:@localhost:3306/pandas_data')  
conn = engine.connect()


# create the first table by numpy random

df = pd.DataFrame(np.random.randint(0, 100, (100, 3)))
df.rename({0 : 'A', 1 : 'B', 2 : 'C'}, axis=1, inplace=True)

# view the first 5 rows
print(df.head())

table_name = 'simple'
df.to_sql(
  table_name,
  engine,
  if_exists='replace',
  index=False,
  chunksize=500,
  dtype={
    'A' : Integer,
    'B' : Integer,
    'C' : Integer
  })



# create the second table from a csv file

# read a csv file
df_csv = pd.read_csv("business_2023.csv")

# view the first 5 rows
print(df_csv.head())


table_name = 'business'
df_csv.to_sql(
  table_name,
  engine,
  if_exists='replace',
  index=False,
  chunksize=500, 
  dtype={
    'Data_value' : Integer, 
    'Magnitude' : Integer,
    'Series_title_5' : Integer
  })