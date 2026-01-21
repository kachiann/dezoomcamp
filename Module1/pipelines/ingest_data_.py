#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# Dtypes and parse_dates (unchanged)
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

@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='yellow_taxi_data', help='Target table name')
@click.option('--year', default=2021, type=int, help='Data year')
@click.option('--month', default=1, type=int, help='Data month (1-12)')
@click.option('--chunk-size', default=100000, type=int, help='CSV chunk size')
def ingest_data(user, password, host, port, db, table, year, month, chunk_size):
    """Ingest NYC yellow taxi data into PostgreSQL."""
    
    # Build connection string
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_string)
    
    # Data URL
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = prefix + f'yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    print(f"Loading from: {url}")
    print(f"Target: {table} in {db}@{host}:{port}")
    
    # Test schema
    df_test = pd.read_csv(url, nrows=100, dtype=dtype, parse_dates=parse_dates)
    print("Test schema OK")
    
    try:
        print(pd.io.sql.get_schema(df_test, name=table, con=engine))
    except Exception as e:
        print(f"Schema preview failed (normal if DB busy): {e}")
    
    # Main ingestion
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
            df_chunk.head(0).to_sql(
                name=table,
                con=engine,
                if_exists="replace",
                index=False
            )
            first = False
            print(f"Table '{table}' created")
        
        df_chunk.to_sql(
            name=table,
            con=engine,
            if_exists="append",
            index=False
        )
        
        rows = len(df_chunk)
        total_rows += rows
        print(f"Inserted {rows:,} rows (total: {total_rows:,})")
    
    print(f"\n✅ Load complete! {total_rows:,} rows in '{table}'")

if __name__ == '__main__':
    ingest_data()
