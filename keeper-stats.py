import pandas as pd
from google.cloud import storage
from datetime import datetime, date, timedelta
import os


# initialize google cloud storage client and associated objects
client = storage.Client()
rewards_bucket = client.get_bucket('entropy-rewards')


# initialize date variables
date = date.today() - timedelta(days=1)
date_str = date.strftime('%Y-%m-%d')
month_str = date.strftime('%Y-%m')


# read in txn time lag sql query
with open ('gbq_entropy_keeper_stats.sql') as query:
    query_string = query.read()


# kick-off the process for writing the previous day's date to gbq and gcs
print(datetime.now(), "Starting the process for the {} txn set...".format(date))

stats_df = pd.read_gbq(query=query_string.format(date_str))


# create a local parquet file
print(datetime.now(), 'Creating parquet file...')   
stats_df.to_parquet('current-keeper-stats.parquet')

# upload the file to google cloud storage
print(datetime.now(), 'Uploading "current" file to GCS...')
blob = rewards_bucket.blob('cumulative/current-keeper-stats.parquet')
blob.upload_from_filename('current-keeper-stats.parquet')

# delete the file from local storage
print(datetime.now(), 'Deleting file from local memory...')
os.remove('current-keeper-stats.parquet')