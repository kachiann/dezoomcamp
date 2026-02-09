import pyarrow.parquet as pq
import gcsfs

fs = gcsfs.GCSFileSystem()

total = 0
for month in ["01","02","03","04","05","06"]:
    path = f"gs://kachi_dezoomcamp_hw3_2026/yellow_tripdata_2024-{month}.parquet"
    table = pq.read_table(path, filesystem=fs)
    total += table.num_rows

print("Total records (Jan–Jun 2024):", total)
