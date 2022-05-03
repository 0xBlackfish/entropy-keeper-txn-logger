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
with open ('gbq_total_rewards.sql') as query:
    query_string = query.read()


# kick-off the process for writing the previous day's date to gbq and gcs
print(datetime.now(), "Starting the process for the {} txn set...".format(date))

rewards_df = pd.read_gbq(query=query_string.format(date_str))
rewards_df['date'] = date_str


# create a local parquet file
print(datetime.now(), 'Creating parquet file...')   
rewards_df[['as_of_date','entropy_keeper_address','total_keeper_reward']].to_parquet('total-rewards-as-of-'+date+'.parquet')

# write the file to google bigquery
print(datetime.now(), 'Writing data to GBQ...')
rewards_df[['as_of_date','entropy_keeper_address','total_keeper_reward']].to_gbq('entropy.keeper_rewards_total',if_exists='append')

# upload the file to google cloud storage
print(datetime.now(), 'Uploading file to GCS...')
blob = rewards_bucket.blob('cumulative/'+date[:7]+'/total-rewards-as-of-'+date+'.parquet')
blob.upload_from_filename('total-rewards-as-of-'+date+'.parquet')

print(datetime.now(), 'Uploading "current" file to GCS...')
blob = rewards_bucket.blob('cumulative/current-total-rewards.parquet')
blob.upload_from_filename('total-rewards-as-of-'+date+'.parquet')

# delete the file from local storage
print(datetime.now(), 'Deleting file from local memory...')
os.remove('total-rewards-as-of-'+date+'.parquet')