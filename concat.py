import glob
import json

import numpy as np
import pandas as pd


CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

# Files
RESOURCES_FOLDER = CONFIG['file']['resources_folder']

YOUR_STREAMING_HISTORY_FILES = 'Streaming_History_Audio_*.json'
YOUR_STREAMING_HISTORY_PATHS = [f for f in glob.glob( RESOURCES_FOLDER + '/' + YOUR_STREAMING_HISTORY_FILES)]

ALL_YOUR_STREAMING_HISTORY_FILE = 'AllStreamingHistory.csv'
ALL_YOUR_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + '/' + ALL_YOUR_STREAMING_HISTORY_FILE


def header_converter(df):
    return df.rename(columns={'endTime': 'end_time', 'msPlayed': 'ms_played', 'artistName': 'artist_name', 'trackName': 'track_name'})


# Read streaming files
df_stream = header_converter(pd.concat(map(pd.read_json, YOUR_STREAMING_HISTORY_PATHS)))
print(f'INFO - {len(df_stream)} rows in {YOUR_STREAMING_HISTORY_PATHS}')

df_stream = df_stream.drop_duplicates()
print(f'INFO - {len(df_stream)} rows without duplicated rows')

df_stream = df_stream.rename(columns={'ts': 'end_time', 'master_metadata_track_name': 'track_name', 'master_metadata_album_artist_name': 'artist_name', 'master_metadata_album_album_name': 'album_name'})
df_stream['track_uri'] = df_stream['spotify_track_uri'].str.split(":", n = 3, expand = True)[2]
df_stream = df_stream.drop(['spotify_track_uri', 'episode_name', 'episode_show_name', 'spotify_episode_uri'], axis=1)

df_stream['track_src_id'] = df_stream.artist_name + ':' + df_stream.track_name
df_stream['year'] = pd.DatetimeIndex(df_stream.end_time).year.map("{:04}".format)
df_stream['month'] = (pd.DatetimeIndex(df_stream.end_time).month).map("{:02}".format)
df_stream['month_name'] = pd.DatetimeIndex(df_stream.end_time).month_name()
df_stream['day'] = pd.DatetimeIndex(df_stream.end_time).day.map("{:02}".format)
df_stream['hour'] = pd.DatetimeIndex(df_stream.end_time).hour.map("{:02}".format)
df_stream['minute'] = pd.DatetimeIndex(df_stream.end_time).minute.map("{:02}".format)
df_stream['min_played'] = df_stream.ms_played / 1000 / 60
df_stream['id'] = df_stream.end_time + ':' + df_stream.track_uri

df_stream['date'] = pd.to_datetime(df_stream.end_time)
df_stream = df_stream.sort_values('date').reset_index(drop=True).drop('date', axis=1)

# Save stream history
df_stream.to_csv(ALL_YOUR_STREAMING_HISTORY_PATH, mode='w', index=False)
print(f'INFO - {len(df_stream)} rows are saved at {ALL_YOUR_STREAMING_HISTORY_PATH}')
