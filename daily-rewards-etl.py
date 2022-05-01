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


# set the file path and read-in the keeper transaction data that will be used to calculate daily rewards
file_path = 'gs://entropy-keeper-transactions/daily/{}/{}-daily-aggregation.parquet'.format(month_str, date_str)
print(datetime.now(), "Starting the process for the {} txn set...".format(date_str))
df = pd.read_parquet(file_path)


# create a column which converts the iso timestamp to datetime
print(datetime.now(), "Formatting and organizing the dataframe...")
df['date_time'] = df['block.timestamp.iso8601'].apply(lambda x: datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))


# sort the dataframe by instruction type and datetime
df_sorted = df.sort_values(by=['instruction_type','date_time'],ascending=[True, True])


# create a column which shows the previous txns datetime relative to the current txn
df_sorted['prev_date_time'] = df_sorted.groupby('instruction_type')['date_time'].shift()


# calculate the time since last txn
df_sorted['time_since_last_txn'] = df_sorted['date_time'] - df_sorted['prev_date_time']


# decompose the time since last txn into time components, convert them to seconds, and add them together
time_components = df_sorted['time_since_last_txn'].dt.components
df_sorted['seconds_since_last_txn'] = (time_components['hours']*3600) + (time_components['minutes']*60) + (time_components['seconds'])
df_sorted['seconds_since_last_txn'].fillna(0,inplace=True)


# calculate the total time for each entropy keeper address
print(datetime.now(), 'Calculating the total time for each entropy keeper address...')
consumeEvents_time_dict = {}
consumeEvents_time = 0
otherEvents_time_dict = {}
otherEvents_time = 0


for index, txn in df_sorted.iterrows():
    wallet = txn['entropy_keeper_address']

    if txn['instruction_type'] == 'ConsumeEvents':
        if wallet not in consumeEvents_time_dict.keys():
            consumeEvents_time_dict[wallet] = 0

        seconds_since_last_txn = txn['seconds_since_last_txn']
        consumeEvents_time_dict[wallet] += seconds_since_last_txn
        consumeEvents_time += seconds_since_last_txn

    else:
        if wallet not in otherEvents_time_dict.keys():
            otherEvents_time_dict[wallet] = 0

        seconds_since_last_txn = txn['seconds_since_last_txn']
        otherEvents_time_dict[wallet] += seconds_since_last_txn
        otherEvents_time += seconds_since_last_txn


# calculat the rewards for each entropy keeper address
print(datetime.now(), 'Calculating the rewards for each entropy keeper address...')
consumeEvents_rewards_dict = {}
otherEvents_rewards_dict = {}

for wallet in consumeEvents_time_dict.keys():
    consumeEvents_rewards_dict[wallet] = rewards_rate_table['consume_events_reward'][date_str] * (consumeEvents_time_dict[wallet] / consumeEvents_time)

for wallet in otherEvents_time_dict.keys():
    otherEvents_rewards_dict[wallet] = rewards_rate_table['other_events_reward'][date_str] * (otherEvents_time_dict[wallet] / otherEvents_time)

otherEvents_rewards_df = pd.DataFrame(list(otherEvents_rewards_dict.items()),columns=['entropy_keeper_address','rewards'])
consumeEvents_rewards_df = pd.DataFrame(list(consumeEvents_rewards_dict.items()),columns=['entropy_keeper_address','rewards'])
total_rewards_df = pd.concat([otherEvents_rewards_df,consumeEvents_rewards_df]).groupby('entropy_keeper_address').sum().sort_values(by='rewards',ascending=False)
total_rewards_df['date'] = date


# write the total reward dataframe into local memory
total_rewards_df.to_parquet(date+'-daily-rewards.parquet')

# upload the file to google cloud storage
print(datetime.now(), 'Uploading file to GCS...')
blob = rewards_bucket.blob('daily/2022-04/'+date_str+'-daily-rewards.parquet')
blob.upload_from_filename(date_str+'-daily-rewards.parquet')

# delete the file from local storage
print(datetime.now(), 'Deleting file from local memory...')
os.remove(date+'-daily-rewards.parquet')