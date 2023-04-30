import json
import os
import time

import numpy as np
import pandas as pd
import requests
from requests.exceptions import HTTPError
from tinydb import TinyDB, Query


CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

CHUNK_SIZE = CONFIG['file']['chunk_size']

# Files
RESOURCES_FOLDER = CONFIG['file']['resources_folder']
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE = 'AllStreamingHistoryToEnrich.csv'
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH = RESOURCES_FOLDER + '/' + ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE

# Spotify's authentication and config
SPOTIFY_CLIENT_ID = CONFIG['spotify']['client_id']
SPOTIFY_CLIENT_SECRET = CONFIG['spotify']['client_secret']
SPOTIFY_AUTH_URL = CONFIG['spotify']['auth_url']
SPOTIFY_BASE_URL = CONFIG['spotify']['base_url']
SPOTIFY_SLEEP = CONFIG['spotify']['s_sleep']
SPOTIFY_TIMEOUT = CONFIG['spotify']['timeout']

auth_response = requests.post(SPOTIFY_AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': SPOTIFY_CLIENT_ID,
    'client_secret': SPOTIFY_CLIENT_SECRET,
}, timeout=SPOTIFY_TIMEOUT)
auth_response_data = auth_response.json()

ACCESS_TOKEN = auth_response_data['access_token']
SPOTIFY_HEADERS = {'Authorization': f'Bearer {ACCESS_TOKEN}'}

# TinyDb
DB_PATH = CONFIG['tinydb']['track_audio_features']
DB = TinyDB(DB_PATH)

# TODO factorize code (enrich.py)

def chunks_iter(iterable, chunk_size):
    """Yield successive n-sized chunks from iter."""
    iterable = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(chunk_size):
                chunk.append(next(iterable))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk
            break

def do_spotify_request(url, headers, params=None):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        print(f' -> {response.request.url}')
        print(f" <- {response.status_code} {response.text[:200].replace(' ', '').encode('UTF-8')}")
        response.raise_for_status()
        return response.json()
    except HTTPError as err:
        if err.response.status_code == 429:
            print(f"WARN - HTTPError - {err} (sleeping {SPOTIFY_SLEEP}s...)")
            time.sleep(SPOTIFY_SLEEP)
            return do_spotify_request(url, headers, params)
        print(f"WARN - HTTPError - {err} (skipping)")
        raise

def get_track_audio_features(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(SPOTIFY_BASE_URL + 'audio-features/', headers=SPOTIFY_HEADERS, params=params)
    return response['audio_features']


def insertInDb(db, data):
    db.insert_multiple(data)


def insertEnrichedTracks(db, tracks_uri, chunk_size):
    for chunk in chunks_iter(tracks_uri, chunk_size):
        track_audio_features = get_track_audio_features(chunk)
        track_audio_features_without_none_value = [taf for taf in track_audio_features if taf]
        insertInDb(db, track_audio_features_without_none_value)


"""
# TODO clean this part
# this part is to drop duplicate uris in the tinydb

maliste=['A', 'B', 'A', 'C', 'E', 'E', 'E']

def get_dup_id(maliste):
    monres = list()
    for i in range(1, len(maliste)):
        if maliste[i] in maliste[:i]:
            monres.append(i)
    return monres

dup_id = get_dup_id(maliste)
db.remove(doc_ids=dup_id)
len(db.all())
"""

# get the audio features of tracks saved it in the TinyDb
df_stream = pd.read_csv(ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH)
print(f'INFO - {len(df_stream)} streams')

track_uris_from_file = set(df_stream['track_uri'])
track_uris_from_db = [track['id'] for track in DB.all()]
audio_feature_uris_to_get = track_uris_from_file.difference(track_uris_from_db)

print(f'INFO - {len(track_uris_from_file)} unique uris')
print(f'INFO - {len(track_uris_from_db)} uris for which the audio features are already retrieved')
print(f'INFO - {len(audio_feature_uris_to_get)} uris for which the audio features must be recovered')

insertEnrichedTracks(DB, audio_feature_uris_to_get, CHUNK_SIZE)

track_uris_from_db = [track['id'] for track in DB.all()]
track_uris_failed_to_get = audio_feature_uris_to_get.difference(track_uris_from_db)
print(f'INFO - {len(track_uris_from_db)} uris for which the audio features are now retrieved')
print(f'INFO - {len(track_uris_failed_to_get)} uris for which the audio features could not be recovered')

#########

# TODO log this part
YOUR_ENRICHED_STREAMING_HISTORY_FILE = 'AllEnrichedStreamingHistory.csv'
YOUR_ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + '/' + YOUR_ENRICHED_STREAMING_HISTORY_FILE
df_already_enriched = pd.read_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH)

taf = pd.DataFrame(DB.all())
taf.drop(['type', 'uri', 'track_href', 'analysis_url', 'duration_ms'], axis=1)

df_wey = df_already_enriched.join(taf.set_index('id'), on='track_uri')
df_wey.to_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH, mode='w', index=False)
