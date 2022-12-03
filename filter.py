import glob
import json
import os

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

RESOURCES_FOLDER = CONFIG['file']['resources_folder']

YOUR_STREAMING_HISTORY_FILES = 'StreamingHistory*.json'
YOUR_STREAMING_HISTORY_PATHS = [f for f in glob.glob( RESOURCES_FOLDER + '/' + YOUR_STREAMING_HISTORY_FILES)]

ALL_YOUR_STREAMING_HISTORY_FILE = 'AllStreamingHistory.csv'
ALL_YOUR_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + '/' + ALL_YOUR_STREAMING_HISTORY_FILE

ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE = 'AllStreamingHistoryToEnrich.csv'
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH = RESOURCES_FOLDER + '/' + ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE

YOUR_ENRICHED_STREAMING_HISTORY_FILE = 'AllEnrichedStreamingHistory.csv'
YOUR_ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + '/' + YOUR_ENRICHED_STREAMING_HISTORY_FILE

df_stream = pd.read_csv(ALL_YOUR_STREAMING_HISTORY_PATH)
print(f'INFO - {len(df_stream)} rows to enrich')

try:
    df_already_enriched = pd.read_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH)
    print(f'INFO - {len(df_already_enriched)} enriched rows found')
    # df_enriched_stream = df_stream[df_stream.track_src_id.isin(df_already_enriched.track_src_id)] # TODO with new column
    df_stream = df_stream[~df_stream.track_src_id.isin(df_already_enriched.track_src_id)]
    print(f'INFO - only {len(df_stream)} rows to enrich')

    # df_enriched_stream.to_csv('todo', mode='w', index_label='index')
    # print(f'INFO - {len(df_enriched_stream)} rows are saved at {"todo"}')

except EmptyDataError:
    print(f'WARN - empty backup file found ({YOUR_ENRICHED_STREAMING_HISTORY_PATH}')
except FileNotFoundError:
    print(f'WARN - no backup file found ({YOUR_ENRICHED_STREAMING_HISTORY_PATH}')

df_stream.to_csv(ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH, mode='w', index_label='index')
print(f'INFO - {len(df_stream)} rows are saved at {ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH}')
