import pandas as pd
from datetime import datetime, timedelta
import os
import requests
import time
from google.cloud import storage
import re


# create utility function to execute queries for bitquery api
def run_query(query, retries=10):
        """
        Query graphQL API.
        If timeerror
        """
        headers = {"X-API-KEY": "BQYCaXaMZlqZrPCSQVsiJrKtxKRVcSe4"}

        retries_counter = 0
        try:
            request = requests.post(
                "https://graphql.bitquery.io/", json={"query": query}, headers=headers
            )
            result = request.json()
            # print(dir(request.content))
            # Make sure that there is no error message
            # assert not request.content.errors
            assert "errors" not in result
        except:
            while (
                (request.status_code != 200
                or "errors" in result)
                and retries_counter < 10
            ):
                print(datetime.now(), f"Retry number {retries_counter}")
                if "errors" in result:
                    print(result["errors"])
                print(datetime.now(), f"Query failed for reason: {request.reason}. sleeping for {150*retries_counter} seconds and retrying...")
                time.sleep(150*retries_counter)
                request = requests.post(
                    "https://graphql.bitquery.io/",
                    json={"query": query},
                    headers=headers,
                )
                retries_counter += 1
            if retries_counter >= retries:
                raise Exception(
                    "Query failed after {} retries and return code is {}.{}".format(
                        retries_counter, request.status_code, query
                    )
                )
        return request.json()


# initialize instruction type dict to map from data.base58 to the instruction type
instruction_type_dict = {
    '5QCjN' : 'CancelAllPerpOrders',
    'BNuyR' : 'CachePrices',
    'BcYfW' : 'PlacePerpOrder',
    'CruFm' : 'CacheRootBanks',
    'HRDyP' : 'ConsumeEvents',
    'QioWX' : 'CachePerpMarkets',
    'SCnns' : 'UpdateFunding',
    'Y8jvF' : 'UpdateRootBank',
    '' : ''
}


# initialize google cloud storage client and variables
client = storage.Client()
bucket = client.get_bucket('entropy-keeper-transactions')
blobs = bucket.list_blobs(prefix='raw/')


# locate the most recently created blob and grab the end datetime from it
most_recent_blob = [blob.name for blob in blobs][-1]
date_string = most_recent_blob.split('/')[2].strip('.parquet')
latest_date = re.findall('\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z',date_string)[-1]


# read-in bitquery query
with open('entropy_instructions_bitquery.txt') as query:
    query_string = query.read()


# initialize bitquery query parameters
after_param = latest_date
till_param = (datetime.strptime(latest_date,'%Y-%m-%dT%H:%M:%SZ')+timedelta(minutes=30)).strftime('%Y-%m-%dT%H:%M:%SZ')


# execute bitquery query
print(datetime.now(),'Running from {} to {}'.format(after_param, till_param))
        
result = run_query(query_string % (after_param, till_param))
print(datetime.now(), 'Query completed!')

df = pd.json_normalize(result['data']['solana']['instructions'])


# if the dataframe is empty, write an empty file to google cloud storage
if df.empty:
    print(datetime.now(), 'Creating empty parquet file...')   
    df.to_parquet(after_param+'-'+till_param+'.parquet')

    print(datetime.now(), 'Uploading file to GCS...')
    blob = bucket.blob('raw/'+datetime.strptime(latest_date,'%Y-%m-%dT%H:%M:%SZ').date().strftime('%Y-%m-%d')+'/'+after_param+'-'+till_param+'.parquet')
    blob.upload_from_filename(after_param+'-'+till_param+'.parquet')

    print(datetime.now(), 'Deleting file from local memory')
    os.remove(after_param+'-'+till_param+'.parquet')

# if the dataframe is not empty, apply logic to add instruction types and reduce the dataframe to a set of six instructions, then write to google cloud storage
else:
    df['data.base58_trunc'] = df['data.base58'].apply(lambda x: x[:5])
    df['instruction_type'] = df['data.base58_trunc'].apply(lambda x: instruction_type_dict[x] if x in instruction_type_dict.keys() else 'other')

    df_reduced = df[df['instruction_type'].isin(['UpdateRootBank','CacheRootBanks','CachePerpMarkets','CachePrices','UpdateFunding','ConsumeEvents'])][['block.height','block.timestamp.iso8601','transaction.feePayer','instruction_type','transaction.signature']].drop_duplicates()
    df_reduced.rename(columns={
        'transaction.feePayer' : 'entropy_keeper_address',
        'transaction.signature' : 'transaction_id'
    }, inplace=True)

    print(datetime.now(), 'Creating parquet file...')       
    df_reduced.to_parquet(after_param+'-'+till_param+'.parquet')

    print(datetime.now(), 'Writing data to GBQ...')
    df_reduced.rename(columns={
        'block.height' : 'block_height',
        'block.timestamp.iso8601' : 'block_timestamp_iso8601'
    }).to_gbq('solana.entropy_keeper_transactions',if_exists='append')

    print(datetime.now(), 'Uploading file to GCS...')
    blob = bucket.blob('raw/'+datetime.strptime(latest_date,'%Y-%m-%dT%H:%M:%SZ').date().strftime('%Y-%m-%d')+'/'+after_param+'-'+till_param+'.parquet')
    blob.upload_from_filename(after_param+'-'+till_param+'.parquet')

    print(datetime.now(), 'Deleting file from local memory')
    os.remove(after_param+'-'+till_param+'.parquet')