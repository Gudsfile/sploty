import glob
import json
import os
import time

import numpy as np
import pandas as pd
import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from pandas.errors import EmptyDataError
from requests.exceptions import HTTPError


CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

CHUNK_SIZE = CONFIG['file']['chunk_size']

# Files
RESOURCES_FOLDER = CONFIG['file']['resources_folder']
YOUR_ENRICHED_STREAMING_HISTORY_FILE = 'AllEnrichedStreamingHistory.csv'
YOUR_ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + '/' + YOUR_ENRICHED_STREAMING_HISTORY_FILE

# Elastic authentication and config
ELASTIC_IS_ENABLED = CONFIG['elasticsearch']['enable']
ELASTIC_HOSTS = CONFIG['elasticsearch']['hosts'] if ELASTIC_IS_ENABLED else None
ELASTIC_AUTH = (
CONFIG['elasticsearch']['username'], CONFIG['elasticsearch']['password']) if ELASTIC_IS_ENABLED else None
ELASTIC_INDICE_NAME = CONFIG['elasticsearch']['indice']['name'] if ELASTIC_IS_ENABLED else None
ELASTIC_INDICE_TYPE = CONFIG['elasticsearch']['indice']['type'] if ELASTIC_IS_ENABLED else None
ELASTIC_INDICE_SETTINGS = CONFIG['elasticsearch']['indice']['settings'] if ELASTIC_IS_ENABLED else None
ELASTIC_INDICE_MAPPINGS = CONFIG['elasticsearch']['indice']['mappings'] if ELASTIC_IS_ENABLED else None
ELASTIC = Elasticsearch(hosts=ELASTIC_HOSTS, basic_auth=ELASTIC_AUTH) if ELASTIC_IS_ENABLED else None


def bulk_factory(df):
    for document in df:
        yield {
            '_index': ELASTIC_INDICE_NAME,
            '_id': document.pop('index'),
            '_source': document
        }


def set_multidata(elastic, data, request_timeout=10):
    print(f' -> bulk {len(data)} documents')
    response = helpers.bulk(elastic, bulk_factory(data), request_timeout=request_timeout)
    print(f' <- bulk response is {response}')


def create_indice_if_not_exist(elastic, index):
    if elastic.indices.exists(index=index):
        print(f'INFO index {index} already exists')
    else:
        print(f'INFO index {index} does not exists')
        request_body = {
            'settings': ELASTIC_INDICE_SETTINGS,
            'mappings': ELASTIC_INDICE_MAPPINGS
        }
        elastic.indices.create(index=index, body=request_body)
        print(f'INFO index {index} created')

if not ELASTIC_IS_ENABLED:
    exit(0)

df_stream = pd.read_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH)

# creates indice
create_indice_if_not_exist(ELASTIC, ELASTIC_INDICE_NAME)

# index streams
print(f'INFO - index {len(df_stream)} tracks')
df_stream['index'] = df_stream.index
json_tmp = json.loads(df_stream.to_json(orient='records'))
set_multidata(ELASTIC, json_tmp)
