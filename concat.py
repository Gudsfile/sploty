import glob
import json

import numpy as np
import pandas as pd


CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

# Files
RESOURCES_FOLDER = CONFIG['file']['resources_folder']

YOUR_STREAMING_HISTORY_FILES = 'StreamingHistory*.json'
YOUR_STREAMING_HISTORY_PATHS = [f for f in glob.glob( RESOURCES_FOLDER + '/' + YOUR_STREAMING_HISTORY_FILES)]

YOUR_LIBRARY_FILE = 'YourLibrary.json'
YOUR_LIBRARY_PATH = RESOURCES_FOLDER + '/' + YOUR_LIBRARY_FILE

YOUR_LIBRARY_TRACKS_FILE = 'YourLibrary_tracks.json'
YOUR_LIBRARY_TRACKS_PATH = RESOURCES_FOLDER + '/' + YOUR_LIBRARY_TRACKS_FILE

ALL_YOUR_STREAMING_HISTORY_FILE = 'AllStreamingHistory.csv'
ALL_YOUR_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + '/' + ALL_YOUR_STREAMING_HISTORY_FILE


def header_converter(df):
    return df.rename(columns={'endTime': 'end_time', 'msPlayed': 'ms_played', 'artistName': 'artist_name', 'trackName': 'track_name'})


# Read streaming files
df_stream = header_converter(pd.concat(map(pd.read_json, YOUR_STREAMING_HISTORY_PATHS)))
print(f'INFO - {len(df_stream)} rows in {YOUR_STREAMING_HISTORY_PATHS}')

df_stream = df_stream.drop_duplicates()
print(f'INFO - {len(df_stream)} rows without duplicated rows')

df_stream['track_src_id'] = df_stream.artist_name + ':' + df_stream.track_name
df_stream['year'] = pd.DatetimeIndex(df_stream.end_time).year.map("{:04}".format)
df_stream['month'] = (pd.DatetimeIndex(df_stream.end_time).month).map("{:02}".format)
df_stream['month_name'] = pd.DatetimeIndex(df_stream.end_time).month_name()
df_stream['day'] = pd.DatetimeIndex(df_stream.end_time).day.map("{:02}".format)
df_stream['hour'] = pd.DatetimeIndex(df_stream.end_time).hour.map("{:02}".format)
df_stream['minute'] = pd.DatetimeIndex(df_stream.end_time).minute.map("{:02}".format)
df_stream['min_played'] = df_stream.ms_played / 1000 / 60

# Read library files
df_library = pd.read_json(YOUR_LIBRARY_TRACKS_PATH)
df_library['track_src_id'] = df_library['artist'] + ':' + df_library['track']
df_uri = df_library['uri'].str.split(':', expand=True)
df_library['track_uri'] = df_uri[2]

# Merge streaming and library data
df_tableau = df_stream.copy()
df_tableau['in_library'] = np.where(df_tableau['track_src_id'].isin(df_library['track_src_id'].tolist()), True, False)
df_tableau = pd.merge(df_tableau, df_library[['album', 'track_src_id', 'track_uri']], how='left', on=['track_src_id'])
df_tableau['date'] = pd.to_datetime(df_tableau.end_time)
df_tableau = df_tableau.sort_values('date').reset_index(drop=True).drop('date', axis=1)

# Save stream history
df_tableau.to_csv(ALL_YOUR_STREAMING_HISTORY_PATH, mode='w', index_label='index')
print(f'INFO - {len(df_stream)} rows are saved at {ALL_YOUR_STREAMING_HISTORY_PATH}')
