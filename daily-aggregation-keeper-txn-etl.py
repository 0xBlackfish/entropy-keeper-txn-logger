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


