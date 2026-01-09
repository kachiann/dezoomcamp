#import sys  # Standard library for CLI args and more

'''print('arguments', sys.argv) # [ 'pipeline.py', '6' ] — argv[0] is script name
month = int(sys.argv[0])
print(f'hello pipeline!, month = {month}')'''

import sys

print('arguments', sys.argv)
if len(sys.argv) < 2:
    print("Usage: python pipeline.py <month>")
    sys.exit(1)
month = int(sys.argv[1])
print(f'Hello pipeline! Month = {month}')


import pandas as pd

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

df.to_parquet(f"output_day_{sys.argv[1]}.parquet")