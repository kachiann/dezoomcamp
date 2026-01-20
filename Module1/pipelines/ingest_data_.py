#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm  # For progress bar

# Define dtypes to save memory and avoid type inference issues
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

# Base URL for NYC TLC yellow taxi data (DataTalksClub mirror)
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'

# Configurable parameters
pg_user = 'root'
pg_password = 'root'
pg_host = 'localhost'
pg_port = 5432
pg_db = 'ny_taxi'
year = 2021
month = 1
chunk_size = 100000

# Full URL for the specific file
url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'

print(f"Loading data from: {url}")

# Create PostgreSQL engine
engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

# Test read 100 rows to inspect schema
df_test = pd.read_csv(url, nrows=100, dtype=dtype, parse_dates=parse_dates)
print("Test data info:")
df_test.info()
print("\nGenerated schema:")
print(pd.io.sql.get_schema(df_test, name='yellow_taxi_data', con=engine))

# Main loading function
def load_data_to_postgres():
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size
    )
    
    first = True
    total_rows = 0
    
    for df_chunk in tqdm(df_iter, desc="Loading chunks"):
        if first:
            # Create table from schema (no data)
            df_chunk.head(0).to_sql(
                name="yellow_taxi_data",
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")
        
        # Append chunk
        df_chunk.to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="append",
            index=False  # No need for index column
        )
        
        rows = len(df_chunk)
        total_rows += rows
        print(f"Inserted {rows} rows (total: {total_rows:,})")
    
    print(f"\nLoad complete! Total rows inserted: {total_rows:,}")

# Run the loader
load_data_to_postgres()
