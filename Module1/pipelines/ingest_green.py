#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
import requests
from sqlalchemy import create_engine
from tqdm.auto import tqdm


def ensure_parquet_available(url: str):
    """Check remote Parquet is reachable and non-empty before reading."""
    resp = requests.head(url, allow_redirects=True)
    size = int(resp.headers.get("Content-Length", 0))

    if resp.status_code != 200 or size == 0:
        raise RuntimeError(
            f"Remote Parquet not available or empty: {url} "
            f"(status={resp.status_code}, size={size})"
        )


@click.command()
@click.option('--user', default='root', help='PostgreSQL user')
@click.option('--password', default='root', help='PostgreSQL password')
@click.option('--host', default='localhost', help='PostgreSQL host')
@click.option('--port', default=5432, type=int, help='PostgreSQL port')
@click.option('--db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--table', default='green_taxi_data', help='Target table name')
@click.option('--year', default=2025, type=int, help='Data year (for remote URL)')
@click.option('--month', default=11, type=int, help='Data month (1-12, for remote URL)')
@click.option('--chunk-size', default=100_000, type=int, help='Chunk size (rows)')
@click.option(
    '--path',
    default='',
    help='Local Parquet path (if set, overrides remote URL)'
)
def ingest_data(user, password, host, port, db, table, year, month, chunk_size, path):
    """Ingest NYC green taxi Parquet data into PostgreSQL."""

    # Build connection string
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(conn_string)

    # Decide source: local file or remote URL
    if path:
        source = path
        print(f"Loading local Parquet file: {source}")
        # No HEAD check for local files
    else:
        source = (
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
            f"green_tripdata_{year}-{month:02d}.parquet"
        )
        print(f"Loading from remote URL: {source}")
        print("Checking remote Parquet availability...")
        ensure_parquet_available(source)
        print("Remote Parquet OK (HTTP 200 and non-zero size)")

    print(f"Target: {table} in {db}@{host}:{port}")

    # Read a small sample to inspect schema
    print("Reading sample for schema...")
    df_sample = pd.read_parquet(source)
    df_sample = df_sample.head(100)
    print("Sample schema:")
    df_sample.info()

    print("\nGenerated schema (from sample):")
    try:
        print(pd.io.sql.get_schema(df_sample, name=table, con=engine))
    except Exception as e:
        print(f"Schema preview failed (will still continue): {e}")

    # Read full Parquet file
    print("\nReading full Parquet file into memory...")
    df = pd.read_parquet(source)
    total_rows = len(df)
    print(f"Total rows in file: {total_rows:,}")

    # Create table schema from empty head
    df.head(0).to_sql(
        name=table,
        con=engine,
        if_exists="replace",
        index=False,
    )
    print(f"Table '{table}' created")

    # Chunk in-memory DataFrame to avoid a single huge INSERT
    start = 0
    with tqdm(total=total_rows, desc="Inserting rows") as pbar:
        while start < total_rows:
            end = min(start + chunk_size, total_rows)
            df_chunk = df.iloc[start:end]

            df_chunk.to_sql(
                name=table,
                con=engine,
                if_exists="append",
                index=False,
            )

            inserted = len(df_chunk)
            pbar.update(inserted)
            print(f"Inserted {inserted:,} rows (rows {start:,}–{end-1:,})")

            start = end

    print(f"\n✅ Load complete! {total_rows:,} rows in '{table}'")


if __name__ == '__main__':
    ingest_data()
