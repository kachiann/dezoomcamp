#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm






dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-03.csv.gz'

df = pd.read_csv(prefix + f'yellow_tripdata_{year}-{month}.csv.gz', nrows=100)

df.head()

df.info()


df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)


get_ipython().system('uv add sqlalchemy psycopg2-binary')


get_ipython().system('uv add psycopg2 --binary')


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

df.head(0)
len(df)

def run():
    pg_user = 'root'
    pg_password = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'

    year = 2021
    month = 1
    chunk_size = 100000 

    df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=100000
    )
for df_chunk in df_iter:
    print(len(df_chunk))

df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


first = True

for df_chunk in df_iter:

    if first:
        # Create table schema (no data)
        df_chunk.head(0).to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))


get_ipython().system('uv add tqdm')


