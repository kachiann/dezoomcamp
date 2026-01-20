#!/usr/bin/env python
# coding: utf-8

# ## Module 1 Homework: Docker & SQL


import pandas as pd


# ### Prepare Data
year = 2025
month = 11

# Download the green taxi trips data for November 2025
get_ipython().system('wget https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet')


# In[5]:


# Read the data
prefix = 'green_tripdata_2025-11.parquet'


# In[6]:


df = pd.read_parquet(prefix)


# In[7]:


df.head()


# In[8]:


df.info()


# In[9]:


df.describe().T


# In[10]:


# Check data types
df.dtypes


# In[11]:


# Check data shape
df.shape


# In[12]:


get_ipython().system('wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv')


# In[13]:


df_zones = pd.read_csv('taxi_zone_lookup.csv')


# In[14]:


df_zones.head()


# In[15]:


df_zones.info()


# ### Question 3: Counting short trips

# In[16]:


mask = (
    (df["lpep_pickup_datetime"] >= "2025-11-01") &
    (df["lpep_pickup_datetime"] <  "2025-12-01") &
    (df["trip_distance"] <= 1)
)

num_trips = df[mask].shape[0]
print(num_trips)


# ### Question 4. Longest trip for each day

# In[17]:


# filter out erroneous distances
df_valid = df[df["trip_distance"] < 100]


# In[18]:


# find row with maximum trip_distance
idx = df_valid["trip_distance"].idxmax()
row = df_valid.loc[idx]


# In[19]:


pickup_day = row["lpep_pickup_datetime"].date()
max_distance = row["trip_distance"]


# In[20]:


print(pickup_day, max_distance)


# ### Question 5. Biggest pickup zone

# In[21]:


get_ipython().system('uv add sqlalchemy psycopg2-binary')


# In[22]:


from sqlalchemy import create_engine


# In[23]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[24]:


print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))


# In[25]:


df.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')


# In[26]:


len(df)


# In[ ]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates = parse_dates,
    iterator=True,
    chunksize=100000
)


# In[27]:


# Filter for November 18th, 2025
df['pickup_date'] = df['lpep_pickup_datetime'].dt.date
df_nov18 = df[df['pickup_date'] == pd.to_datetime('2025-11-18').date()]


# In[28]:


print(f"Total trips on November 18th, 2025: {len(df_nov18)}")


# In[29]:


# Join with zone names
df_joined = df_nov18.merge(
    df_zones,
    left_on='PULocationID',
    right_on='LocationID',
    how='left'
)


# In[30]:


# Calculate total_amount by pickup zone
zone_totals = df_joined.groupby('Zone')['total_amount'].sum().sort_values(ascending=False)


# In[31]:


print("\nTop 10 pickup zones by total_amount on November 18th, 2025:")
print(zone_totals.head(10))


# In[32]:


print(f"\n🎯 ANSWER: {zone_totals.idxmax()}")
print(f"Total amount: ${zone_totals.max():.2f}")


# In[33]:


# Join to get pickup zone names
df_with_pu_zones = df.merge(
    df_zones,
    left_on='PULocationID',
    right_on='LocationID',
    how='left',
    suffixes=('', '_pickup')
).rename(columns={'Zone': 'PUZone'})


# In[34]:


# Filter for East Harlem North pickups
df_ehn = df_with_pu_zones[df_with_pu_zones['PUZone'] == 'East Harlem North']


# In[35]:


print(f"Total trips from East Harlem North in November 2025: {len(df_ehn)}")


# In[36]:


# Join to get dropoff zone names
df_ehn_with_do = df_ehn.merge(
    df_zones,
    left_on='DOLocationID',
    right_on='LocationID',
    how='left',
    suffixes=('', '_dropoff')
).rename(columns={'Zone': 'DOZone'})


# In[37]:


# Find the trip with the largest tip
max_tip_idx = df_ehn_with_do['tip_amount'].idxmax()
max_tip_trip = df_ehn_with_do.loc[max_tip_idx]


# In[38]:


print(f"\n🎯 ANSWER: {max_tip_trip['DOZone']}")
print(f"Tip amount: ${max_tip_trip['tip_amount']:.2f}")
print(f"Pickup: {max_tip_trip['lpep_pickup_datetime']}")
print(f"Total fare: ${max_tip_trip['total_amount']:.2f}")


# In[ ]:




