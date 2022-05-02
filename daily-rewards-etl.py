import pandas as pd
from google.cloud import storage
from datetime import datetime, date, timedelta
import os


# initialize google cloud storage client and associated objects
client = storage.Client()
txn_bucket = client.get_bucket('entropy-keeper-transactions')
rewards_bucket = client.get_bucket('entropy-rewards')
rewards_rate_table = pd.read_parquet('gs://entropy-rewards/program-references/2022-05-01-rewards-rate-table.parquet')
rewards_rate_table['date'] = rewards_rate_table['date'].apply(lambda x: pd.to_datetime(x))
rewards_rate_table.set_index('date',inplace=True)


# initialize date variables
date = date.today() - timedelta(days=1)
date_str = date.strftime('%Y-%m-%d')
month_str = date.strftime('%Y-%m')


# read in txn time lag sql query
with open ('gbq_txn_time_lag.sql') as query:
    query_string = query.read()


# kick-off the process for writing the previous day's date to gbq and gcs
print(datetime.now(), "Starting the process for the {} txn set...".format(date))

rewards_df = pd.read_gbq(query=query_string.format(date_str))

rewards_df['daily_total_reward'] = rewards_df['instruction_type'].apply(
    lambda x: rewards_rate_table.loc[date]['consume_events_reward'] if x == 'ConsumeEvents' else rewards_rate_table.loc[date]['other_events_reward']
)

rewards_df['daily_keeper_reward'] = rewards_df['pct_of_time'] * rewards_df['daily_total_reward']
rewards_df['date'] = date_str


# create a local parquet file
print(datetime.now(), 'Creating parquet file...')   
rewards_df.to_parquet(date_str+'-daily-rewards.parquet')

# write the file to google big query
print(datetime.now(), 'Writing data to GBQ...')
rewards_df.to_gbq('entropy.keeper_rewards_daily',if_exists='append')

# upload the file to google cloud storage
print(datetime.now(), 'Uploading file to GCS...')
blob = rewards_bucket.blob('daily/'+month_str+'/'+date_str+'-daily-rewards.parquet')
blob.upload_from_filename(date+'-daily-rewards.parquet')

# delete the file from local storage
print(datetime.now(), 'Deleting file from local memory...')
os.remove(date+'-daily-rewards.parquet')