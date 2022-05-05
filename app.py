import json
import glob
import os
import time

import pandas as pd
import numpy as np
import requests


INDEX = '3'

RESOURCES_FOLDER = 'resources/my_spotify_data_'
LAST_RESOURCES_FOLDER = 'resources/my_spotify_data_' + INDEX
RESULTS_FOLDER = 'results/my_spotify_data_' + INDEX

CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

# Spotify Authentication
AUTH_URL = CONFIG['AUTH_URL']
SPOTIFY_CLIENT_ID = CONFIG['SPOTIFY_CLIENT_ID']
SPOTIFY_CLIENT_SECRET = CONFIG['SPOTIFY_CLIENT_SECRET']

auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': SPOTIFY_CLIENT_ID,
    'client_secret': SPOTIFY_CLIENT_SECRET,
})
auth_response_data = auth_response.json()

ACCESS_TOKEN = auth_response_data['access_token']

# Spotify bases
HEADERS = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
BASE_URL = 'https://api.spotify.com/v1/'


def chunks_list(lst, chunk_size):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


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
        if params:
            response = requests.get(url, headers=headers, params=params)
        else:
            response = requests.get(url, headers=headers)
        print(f' -> {response.request.url}')
        print(f" <- {response.status_code} {response.text[:200].replace(' ', '').encode('UTF-8')}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"WARNING - HTTPError - {err}")
        time.sleep(5)
        return do_spotify_request(url, headers, params)


def get_track_uri(track_name, artist_name):
    params = [
        ('q', artist_name + ' track:' + track_name.replace('\'', ' ')),
        ('type', 'track'),
        ('market', 'FR'),
        ('limit', '1'),
        ('offset', '0')
    ]
    response = do_spotify_request(BASE_URL + 'search/', headers=HEADERS, params=params)
    track = response['tracks']['items'][0]
    track_uri = track['uri'].split(':')[2]
    # a_uri = result_track['artists'][0]['uri'].split(':')[2]
    # track_popularity = result_track.get('popularity', None)
    # track_duration_ms = result_track.get('duration_ms', None)
    return track_uri


def get_artist_uris(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(BASE_URL + 'tracks/', headers=HEADERS, params=params)
    tracks = response['tracks']
    artist_uris = [artists['artists'][0]['uri'].split(':')[2] for artists in tracks]
    return artist_uris


def get_artist_data(artist_uris: list):
    params = [
        ('ids', ','.join(artist_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(BASE_URL + 'artists/', headers=HEADERS, params=params)
    artists = response['artists']
    artist_genres = [artist.get('genres', None) for artist in artists]
    artist_popularity = [artist.get('popularity', None) for artist in artists]
    return artist_genres, artist_popularity


def get_track_data(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(BASE_URL + 'tracks/', headers=HEADERS, params=params)
    tracks = response['tracks']
    track_durations_ms = [track.get('duration_ms', None) for track in tracks]
    track_popularity = [track.get('popularity', None) for track in tracks]
    return track_durations_ms, track_popularity


def get_track_audio_features(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(BASE_URL + 'audio-features/', headers=HEADERS, params=params)
    return [{
        'danceability': track_af.get('danceability', None),
        'energy': track_af.get('energy', None),
        'key': track_af.get('key', None),
        'loudness': track_af.get('loudness', None),
        'mode': track_af.get('mode', None),
        'speechiness': track_af.get('speechiness', None),
        'acousticness': track_af.get('acousticness', None),
        'instrumentalness': track_af.get('instrumentalness', None),
        'liveness': track_af.get('liveness', None),
        'valence': track_af.get('valence', None),
        'tempo': track_af.get('tempo', None)
    } for track_af in response['audio_features']]


def enrich_track_uri(row):
    index = row[0]
    stream = row[1]
    track_uri = stream['track_uri']
    track_name = stream['trackName']
    artist_name = stream['artistName']

    print(f'INFO - enrich track uri n°{index} ({track_uri})')

    # if track uri is nan
    if isinstance(track_uri, float):
        row[1]['track_uri'] = get_track_uri(track_name, artist_name)

    return row


def enrich_artist_uri(rows):
    print('INFO - enrich artist uri')
    artist_uris = get_artist_uris([row[1]['track_uri'] for row in rows])

    for i in range(50):
        rows[i][1]['artist_uri'] = artist_uris[i]

    return rows


def enrich_track_data(rows):
    print('INFO - enrich track data')
    track_durations_ms, track_popularity = get_track_data([row[1]['track_uri'] for row in rows])

    for i in range(50):
        rows[i][1]['track_duration_ms'] = track_durations_ms[i]
        rows[i][1]['track_popularity'] = track_popularity[i]

    return rows


def enrich_artist_data(rows):
    print('INFO - enrich artist data')
    artist_genres, artist_popularity = get_artist_data([row[1]['artist_uri'] for row in rows])

    for i in range(50):
        rows[i][1]['artist_genres'] = artist_genres[i]
        rows[i][1]['artist_popularity'] = artist_popularity[i]

    return rows


def enrich_track_audio_features(rows):
    print('INFO - enrich audio features')
    track_af = get_track_audio_features([row[1]['track_uri'] for row in rows])

    for i in range(50):
        rows[i][1]['audio_features'] = track_af[i]

    return rows


def app():
    resources_files = [f for f in glob.glob(RESOURCES_FOLDER + '*/StreamingHistory*.json')]
    df_stream = pd.concat(map(pd.read_json, resources_files)).drop_duplicates()

    df_stream['UniqueID'] = df_stream['artistName'] + ':' + df_stream['trackName']

    ##

    df_library = pd.read_json(LAST_RESOURCES_FOLDER + '/YourLibrary_tracks.json')

    df_library['UniqueID'] = df_library['artist'] + ":" + df_library['track']

    new = df_library["uri"].str.split(":", expand=True)
    df_library['track_uri'] = new[2]

    ##

    df_tableau = df_stream.copy()

    df_tableau['In Library'] = np.where(df_tableau['UniqueID'].isin(df_library['UniqueID'].tolist()), 1, 0)

    df_tableau = pd.merge(df_tableau, df_library[['album', 'UniqueID', 'track_uri']], how='left', on=['UniqueID'])

    dict_all = {}

    df_tableau = df_tableau.reset_index()
    for rows in chunks_iter(df_tableau.iterrows(), 50):

        rows = list(map(enrich_track_uri, rows))
        rows = enrich_artist_uri(rows)
        rows = enrich_artist_data(rows)
        rows = enrich_track_data(rows)
        rows = enrich_track_audio_features(rows)

        for row in rows:
            dict_all[row[0]] = row[1]

    dict_all = pd.DataFrame.from_dict(dict_all, orient='index')
    dict_all.reset_index(inplace=True, drop=True)
    dict_all.head()

    ##

    if not os.path.exists(RESULTS_FOLDER):
        os.mkdir(RESULTS_FOLDER)

    dict_all.to_csv(RESULTS_FOLDER + '/v2.csv')


if __name__ == '__main__':
    app()
    print('done')
