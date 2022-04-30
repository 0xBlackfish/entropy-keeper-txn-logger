import pandas as pd
from google.cloud import storage
from google.api_core import page_iterator
from datetime import datetime, date, timedelta
import os


# create utility functions to pull directory names from google cloud storage file structure
def _item_to_value(iterator, item):
    return item

def list_directories(bucket_name, prefix):
    if prefix and not prefix.endswith('/'):
        prefix += '/'

    extra_params = {
        "projection": "noAcl",
        "prefix": prefix,
        "delimiter": '/'
    }

    gcs = storage.Client()

    path = "/b/" + bucket_name + "/o"

    iterator = page_iterator.HTTPIterator(
        client=gcs,
        api_request=gcs._connection.api_request,
        path=path,
        items_key='prefixes',
        item_to_value=_item_to_value,
        extra_params=extra_params,
    )

    return [x for x in iterator]


# initialize google cloud storage client and variables
client = storage.Client()
bucket = client.get_bucket('entropy-keeper-transactions')


# set date to run on a 15 minute lag against the current datetime
# this ensures that the final 30 minute window (23:30 to 0:00) of a day gets captured
date = (datetime.now() - timedelta(minutes=45)).date().strftime('%Y-%m-%d')
month = (datetime.now() - timedelta(minutes=45)).date().strftime('%Y-%m')
print(datetime.now(), 'Starting on {}'.format(date))
df_daily_agg = pd.DataFrame()

# gather all the blobs from the entropy keeper transactions bucket for a given day
blobs = bucket.list_blobs(prefix='raw/'+date)

# iterate through the blobs incrementally adding them to the df_daily_agg dataframe
for blob in blobs:
    file_path = 'gs://entropy-keeper-transactions/{}'.format(blob.name)

    df_blob = pd.read_parquet(file_path)
    
    df_daily_agg = pd.concat([df_daily_agg, df_blob], axis=0)

df_daily_agg.to_parquet(date+'-daily-aggregation.parquet')

# write the dataframe to google cloud storage as a parquet file
print(datetime.now(), 'Uploading file to GCS...')
blob = bucket.blob('daily/'+month+'/'+date+'-daily-aggregation.parquet')
blob.upload_from_filename(date+'-daily-aggregation.parquet')

# delete the file from local memory
print(datetime.now(), 'Deleting file from local memory')
os.remove(date+'-daily-aggregation.parquet')