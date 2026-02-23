"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb_local

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
    description: "Pickup TLC Taxi Zone ID"
  - name: dropoff_location_id
    type: integer
    description: "Dropoff TLC Taxi Zone ID"
  - name: fare_amount
    type: double
    description: "Fare amount"
  - name: total_amount
    type: double
    description: "Total amount charged"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: taxi_type
    type: string
    description: "Taxi type (fixed: yellow)"
  - name: extracted_at
    type: timestamp
    description: "UTC timestamp when data was pulled"
@bruin"""

import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _month_starts(start_date: str, end_date: str) -> list[tuple[int, int]]:
    start = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)

    cur = pd.Timestamp(year=start.year, month=start.month, day=1)
    last = pd.Timestamp(year=end.year, month=end.month, day=1)

    months = []
    while cur <= last:
        months.append((cur.year, cur.month))
        cur = cur + pd.offsets.MonthBegin(1)
    return months


def _read_parquet_from_url(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    return pd.read_parquet(BytesIO(r.content))


def materialize() -> pd.DataFrame:
    start_date = os.environ["BRUIN_START_DATE"]  # YYYY-MM-DD
    end_date = os.environ["BRUIN_END_DATE"]      # YYYY-MM-DD

    extracted_at = datetime.now(timezone.utc)
    taxi_type = "yellow"

    dfs: list[pd.DataFrame] = []
    for year, month in _month_starts(start_date, end_date):
        url = f"{BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"

        try:
            df = _read_parquet_from_url(url)
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            # 404 = missing; 403 = commonly returned for not-yet-published months
            if status in (403, 404):
                continue
            raise

        # Yellow schema datetime columns
        df["pickup_datetime"] = pd.to_datetime(df.get("tpep_pickup_datetime"), errors="coerce")
        df["dropoff_datetime"] = pd.to_datetime(df.get("tpep_dropoff_datetime"), errors="coerce")

        # Standardize columns that staging expects
        df["pickup_location_id"] = df.get("PULocationID")
        df["dropoff_location_id"] = df.get("DOLocationID")
        df["fare_amount"] = df.get("fare_amount")
        df["total_amount"] = df.get("total_amount")
        df["payment_type"] = df.get("payment_type")

        df["taxi_type"] = taxi_type
        df["extracted_at"] = pd.Timestamp(extracted_at)

        # Filter to run window (inclusive start day, exclusive next day after end_date)
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1)
        df = df[(df["pickup_datetime"] >= start_ts) & (df["pickup_datetime"] < end_ts)]

        # Keep ONLY stable schema columns (prevents schema drift)
        df = df[
            [
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id",
                "fare_amount",
                "total_amount",
                "payment_type",
                "taxi_type",
                "extracted_at",
            ]
        ]

        dfs.append(df)

    if not dfs:
        return pd.DataFrame(
            {
                "pickup_datetime": pd.Series(dtype="datetime64[ns]"),
                "dropoff_datetime": pd.Series(dtype="datetime64[ns]"),
                "pickup_location_id": pd.Series(dtype="Int64"),
                "dropoff_location_id": pd.Series(dtype="Int64"),
                "fare_amount": pd.Series(dtype="float64"),
                "total_amount": pd.Series(dtype="float64"),
                "payment_type": pd.Series(dtype="Int64"),
                "taxi_type": pd.Series(dtype="object"),
                "extracted_at": pd.Series(dtype="datetime64[ns]"),
            }
        )

    return pd.concat(dfs, ignore_index=True)