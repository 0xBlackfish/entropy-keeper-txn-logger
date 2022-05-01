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


# initialize an empty dataframe to aggregate all daily rewards
agg_rewards_df = pd.DataFrame()


# call list of all blobs in the entropy rewards bucket, daily folder
blobs = rewards_bucket.list_blobs(prefix='daily/2')

for blob in blobs:
    print(datetime.now(), "Adding {} to the dataframe...".format(file_path))
    file_path = 'gs://entropy-rewards/{}'.format(blob.name)
    
    temp_df = pd.read_parquet(file_path)
    
    agg_rewards_df = pd.concat([agg_rewards_df,temp_df])

print(datetime.now(), "Aggregating rewards data into the cumulative dataframe...")
cumulative_rewards = agg_rewards_df.groupby('entropy_keeper_address').sum().sort_values(by='rewards',ascending=False)
cumulative_rewards.to_parquet('total-rewards-as-of-'+date_str)

# upload the file to google cloud storage
print(datetime.now(), 'Uploading file to GCS...')
blob = rewards_bucket.blob('cumulative/'+month_str+'/total-rewards-as-of-'+date_str)
blob.upload_from_filename('total-rewards-as-of-'+date_str)

# delete the file from local storage
print(datetime.now(), 'Deleting file from local memory...')
os.remove('total-rewards-as-of-'+date_str)