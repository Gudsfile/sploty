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

##

CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

# Files

INDEX = CONFIG['file']['index']

RESOURCES_FOLDER = CONFIG['file']['resources_folder']
LAST_RESOURCES_FOLDER = RESOURCES_FOLDER + '_' + INDEX
RESULTS_FOLDER = CONFIG['file']['results_folder'] + '_' + INDEX
RESULT_FILE = CONFIG['file']['result_file']

CHUNK_SIZE = CONFIG['file']['chunk_size']

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

# Spotify's authentication and config
SPOTIFY_CLIENT_ID = CONFIG['spotify']['client_id']
SPOTIFY_CLIENT_SECRET = CONFIG['spotify']['client_secret']
SPOTIFY_AUTH_URL = CONFIG['spotify']['auth_url']
SPOTIFY_BASE_URL = CONFIG['spotify']['base_url']
SPOTIFY_SLEEP = CONFIG['spotify']['s_sleep']

auth_response = requests.post(SPOTIFY_AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': SPOTIFY_CLIENT_ID,
    'client_secret': SPOTIFY_CLIENT_SECRET,
})
auth_response_data = auth_response.json()

ACCESS_TOKEN = auth_response_data['access_token']
SPOTIFY_HEADERS = {'Authorization': f'Bearer {ACCESS_TOKEN}'}


# Color
class BoldColor:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


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
        response = requests.get(url, headers=headers, params=params)
        print(f' -> {response.request.url}')
        print(f" <- {response.status_code} {response.text[:200].replace(' ', '').encode('UTF-8')}")
        response.raise_for_status()
        return response.json()
    except HTTPError as err:
        # Todo inverser, par défaut on remonte l'erreur dans des cas particuliers on fait une action
        if err.response.status_code == 404:
            print(f"WARN - HTTPError - {err} (skipping)")
            raise
        else:
            print(f"WARN - HTTPError - {err} (sleeping {SPOTIFY_SLEEP}s...)")
            time.sleep(SPOTIFY_SLEEP)
            return do_spotify_request(url, headers, params)


def get_track_uri(track_name, artist_name):
    params = [
        ('q', artist_name + ' track:' + track_name.replace('\'', ' ')),
        ('type', 'track'),
        ('market', 'FR'),
        ('limit', '1'),
        ('offset', '0')
    ]
    response = do_spotify_request(SPOTIFY_BASE_URL + 'search/', headers=SPOTIFY_HEADERS, params=params)
    try:
        track = response['tracks']['items'][0]
        track_uri = track['uri'].split(':')[2]
    except IndexError as err:
        print(f"WARNING - IndexError - {err}")
        track_uri = None
    return track_uri


def get_artist_uris(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(SPOTIFY_BASE_URL + 'tracks/', headers=SPOTIFY_HEADERS, params=params)
    tracks = response['tracks']
    artist_uris = [artists['artists'][0]['uri'].split(':')[2] if artists else None for artists in tracks]
    return artist_uris


def get_artist_data(artist_uris: list):
    params = [
        ('ids', ','.join(artist_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(SPOTIFY_BASE_URL + 'artists/', headers=SPOTIFY_HEADERS, params=params)
    artists = response['artists']
    artist_genres = [artist.get('genres', None) if artist else [] for artist in artists]
    artist_popularity = [artist.get('popularity', None) if artist else None for artist in artists]
    return artist_genres, artist_popularity


def get_track_data(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(SPOTIFY_BASE_URL + 'tracks/', headers=SPOTIFY_HEADERS, params=params)
    tracks = response['tracks']
    track_durations_ms = [track.get('duration_ms', None) if track else None for track in tracks]
    track_popularity = [track.get('popularity', None) if track else None for track in tracks]
    return track_durations_ms, track_popularity


def get_track_audio_features(track_uris: list):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    response = do_spotify_request(SPOTIFY_BASE_URL + 'audio-features/', headers=SPOTIFY_HEADERS, params=params)
    return [{
        'danceability': track_af.get('danceability', None) if track_af else None,
        'energy': track_af.get('energy', None) if track_af else None,
        'key': track_af.get('key', None) if track_af else None,
        'loudness': track_af.get('loudness', None) if track_af else None,
        'mode': track_af.get('mode', None) if track_af else None,
        'speechiness': track_af.get('speechiness', None) if track_af else None,
        'acousticness': track_af.get('acousticness', None) if track_af else None,
        'instrumentalness': track_af.get('instrumentalness', None) if track_af else None,
        'liveness': track_af.get('liveness', None) if track_af else None,
        'valence': track_af.get('valence', None) if track_af else None,
        'tempo': track_af.get('tempo', None) if track_af else None
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
    artist_uris = get_artist_uris([row[1]['track_uri'] if row[1]['track_uri'] else 'NaN' for row in rows])

    for i, row in enumerate(rows):
        row[1]['artist_uri'] = artist_uris[i]

    return rows


def enrich_track_data(rows):
    print('INFO - enrich track data')
    track_durations_ms, track_popularity = get_track_data(
        [row[1]['track_uri'] if row[1]['track_uri'] else 'NaN' for row in rows])

    for i, row in enumerate(rows):
        row[1]['track_duration_ms'] = track_durations_ms[i]
        row[1]['track_popularity'] = track_popularity[i]

    return rows


def enrich_artist_data(rows):
    print('INFO - enrich artist data')
    artist_genres, artist_popularity = get_artist_data(
        [row[1]['artist_uri'] if row[1]['artist_uri'] else 'NaN' for row in rows])

    for i, row in enumerate(rows):
        row[1]['artist_genres'] = artist_genres[i]
        row[1]['artist_popularity'] = artist_popularity[i]

    return rows


def enrich_track_audio_features(rows):
    print('INFO - enrich audio features')
    track_af = get_track_audio_features([row[1]['track_uri'] if row[1]['track_uri'] else 'NaN' for row in rows])

    for i, row in enumerate(rows):
        row[1]['audio_features'] = track_af[i]

    return rows


def bulk_factory(df):
    for document in df:
        yield {
            '_index': ELASTIC_INDICE_NAME,
            '_id': document['index'],
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


def another_get(track_uris):
    params = [
        ('ids', ','.join(track_uris)),
        ('type', 'track'),
        ('market', 'FR')
    ]
    return do_spotify_request(SPOTIFY_BASE_URL + 'tracks/', headers=SPOTIFY_HEADERS, params=params)


def better_get(track_name, artist_name):
    params = [
        ('q', artist_name + ' track:' + track_name.replace('\'', ' ')),
        ('type', 'track'),
        ('market', 'FR'),
        ('limit', '1'),
        ('offset', '0')
    ]
    return do_spotify_request(SPOTIFY_BASE_URL + 'search/', headers=SPOTIFY_HEADERS, params=params)


def merger(df1, df5):
    df1['is_done'] = df1.unique_id.isin(df5.unique_id)
    df = df1.merge(df5, on='unique_id', how='left')

    for c_x in df.columns:
        if c_x.endswith('_x'):
            c = c_x.removesuffix('_x')
            c_y = c + '_y'
            df[c] = df[c_y].where(df.is_done, df[c_x])
    return df.loc[:, ~df.columns.str.contains('_x$|_y$')]


def saver(df_tableau, complete_data):
    complete_data = pd.DataFrame.from_dict(complete_data, orient='index')
    complete_data.reset_index(inplace=True, drop=True)

    toto = merger(df_tableau, complete_data)
    to_write = toto[toto['is_done'] == True]
    to_keep = toto[toto['is_done'] == False]

    # writes data in csv file
    if not os.path.exists(RESULTS_FOLDER):
        os.mkdir(RESULTS_FOLDER)
    to_write.to_csv(RESULTS_FOLDER + RESULT_FILE, mode='a', header=not os.path.exists(RESULTS_FOLDER + RESULT_FILE))

    return to_keep.reset_index(drop=True)


def better_enrich(df_tableau):
    print(f'INFO - enrich track data for {len(df_tableau)} tracks')

    df = df_tableau[['track_uri', 'trackName', 'artistName', 'unique_id']].drop_duplicates('unique_id')
    df = df.reset_index()

    print(f'INFO - reduce enrich for only {len(df)} tracks')
    rows_with_track_uri = df[df['track_uri'].notna()]
    rows_without_track_uri = df[df['track_uri'].isna()]

    # if track uri is nan
    print(f'INFO - enrich track data and uri for {len(rows_without_track_uri)} tracks')
    dict_all = {}
    target = len(rows_without_track_uri)
    step = CHUNK_SIZE * 10
    checkpoint = 0
    for rows in chunks_iter(rows_without_track_uri.iterrows(), CHUNK_SIZE):
        print(' ' * 40 + BoldColor.PURPLE + '[' + '-' * int(checkpoint / step) + ' ' * int(
            (target - checkpoint) / step) + ']' + BoldColor.DARKCYAN + f' {checkpoint}/{target}' + BoldColor.END)
        for row in rows:
            try:
                index = row[0]
                stream = row[1]

                response = better_get(stream['trackName'], stream['artistName'])

                try:
                    track = response['tracks']['items'][0]
                    track_uri = track['uri'].split(':')[2]
                except IndexError as err:
                    print(f"WARNING - IndexError - {err}")
                    continue

                track_uri = track['uri'].split(':')[2]
                artist = track['artists'][0]  # only one artist :(
                album = track['album']

                print(f'DEBUG - enrich track uri n°{index} (NaN -> {track_uri})')

                stream['track_uri'] = track_uri
                stream['artist_uri'] = artist['uri'].split(':')[2]
                stream['track_duration_ms'] = track.get('duration_ms', None)
                stream['track_popularity'] = track.get('popularity', None)
                dict_all[index] = stream
            except HTTPError as err:
                print(f"WARNING - HTTPError - {err} - for  {row}")
                continue

        checkpoint += CHUNK_SIZE
        df_tableau = saver(df_tableau, dict_all)
        dict_all = {}

    # if track already have an uri
    print(f'INFO - enrich track data for {len(rows_with_track_uri)} tracks')
    target = len(rows_with_track_uri)
    step = CHUNK_SIZE * 10
    checkpoint = 0
    for rows in chunks_iter(rows_with_track_uri.iterrows(), CHUNK_SIZE):
        print(' ' * 40 + BoldColor.PURPLE + '[' + '-' * int(checkpoint / step) + ' ' * int((target - checkpoint) / step) + ']' + BoldColor.DARKCYAN + f' {checkpoint}/{target}' + BoldColor.END)
        response = another_get([row[1]['track_uri'] for row in rows])  # il doit y avoir mieux
        for i, row in enumerate(rows):
            index = row[0]
            stream = row[1]

            track = response['tracks'][i]
            artist = track['artists'][0]  # only one artist :(
            album = track['album']
            track_uri = track['uri']

            print(f'DEBUG - enrich track uri n°{index} ({track_uri})')

            stream['artist_uri'] = artist['uri'].split(':')[2]
            stream['track_duration_ms'] = track.get('duration_ms', None)
            stream['track_popularity'] = track.get('popularity', None)
            dict_all[index] = stream
        checkpoint += CHUNK_SIZE
        df_tableau = saver(df_tableau, dict_all)
        dict_all = {}


def app():
    # cerates indice
    if ELASTIC_IS_ENABLED:
        create_indice_if_not_exist(ELASTIC, ELASTIC_INDICE_NAME)

    # reads streaming files
    resources_files = [f for f in glob.glob(RESOURCES_FOLDER + '*/StreamingHistory*.json')]
    df_stream = pd.concat(map(pd.read_json, resources_files)).drop_duplicates()
    df_stream['unique_id'] = df_stream['artistName'] + ':' + df_stream['trackName']

    # reads library files
    df_library = pd.read_json(LAST_RESOURCES_FOLDER + '/YourLibrary_tracks.json')
    df_library['unique_id'] = df_library['artist'] + ':' + df_library['track']
    new = df_library["uri"].str.split(":", expand=True)
    df_library['track_uri'] = new[2]

    # merges streaming and library data
    df_tableau = df_stream.copy()
    df_tableau['in_library'] = np.where(df_tableau['unique_id'].isin(df_library['unique_id'].tolist()), True, False)
    df_tableau = pd.merge(df_tableau, df_library[['album', 'unique_id', 'track_uri']], how='left', on=['unique_id'])
    df_tableau = df_tableau.reset_index()

    # get already saved data
    print(f'INFO - {len(df_tableau)} rows to enrich')
    try:
        saved_df = pd.read_csv('results/my_spotify_data_5testing_file.csv')
        print(f'INFO - {len(saved_df)} rows founds')
        df_tableau = df_tableau[~df_tableau.unique_id.isin(saved_df.unique_id)]
        print(f'INFO - only {len(df_tableau)} rows to enrich')
    except EmptyDataError:
        print('WARN - empty backup file found')
    except FileNotFoundError:
        print('WARN - no backup file found')

    ############################# TODO
    # redesign enrichment part
    # piste: récupérer que les artistes en 1 seul exemplaire, faire les requêtes
    #        et joindre sur l'artiste avec le df pour remplir toutes les occu d'un artiste d'un coup
    #        1 recherche / artiste
    # with_track_uri = df[df['track_uri'].notna()]
    # artists = parcoureur(with_track_uri[['track_uri']])
    # .drop_duplicates('unique_id')

    # enriches the data and indexes it
    better_enrich(df_tableau)

    # rows = enrich_artist_data(rows)
    # rows = enrich_track_audio_features(rows)


if __name__ == '__main__':
    app()
    print('done')
