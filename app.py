import json
import os

import pandas as pd
import numpy as np
import requests

# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html
# https://towardsdatascience.com/get-your-spotify-streaming-history-with-python-d5a208bbcbd3

index = '3'

last_resources_folder = 'resources/my_spotify_data_' + index
results_folder = 'results/my_spotify_data_' + index

config_filde = 'config.json'

##

resources_folder = 'resources/my_spotify_data_'

df_stream10 = pd.read_json(resources_folder + '1/StreamingHistory0.json')
df_stream11 = pd.read_json(resources_folder + '1/StreamingHistory1.json')
df_stream12 = pd.read_json(resources_folder + '1/StreamingHistory2.json')

df_stream20 = pd.read_json(resources_folder + '2/StreamingHistory0.json')
df_stream21 = pd.read_json(resources_folder + '2/StreamingHistory1.json')
df_stream22 = pd.read_json(resources_folder + '2/StreamingHistory2.json')

df_stream30 = pd.read_json(resources_folder + '3/StreamingHistory0.json')
df_stream31 = pd.read_json(resources_folder + '3/StreamingHistory1.json')
df_stream32 = pd.read_json(resources_folder + '3/StreamingHistory2.json')
df_stream33 = pd.read_json(resources_folder + '3/StreamingHistory3.json')

df_stream1 = pd.concat([df_stream10, df_stream11, df_stream12])
df_stream2 = pd.concat([df_stream20, df_stream21, df_stream22])
df_stream3 = pd.concat([df_stream30, df_stream31, df_stream32, df_stream33])

df_stream = pd.concat([df_stream1, df_stream2, df_stream3])

df_stream = df_stream.drop_duplicates()

df_stream['UniqueID'] = df_stream['artistName'] + ':' + df_stream['trackName']

df_stream.head()

##

df_library = pd.read_json(last_resources_folder + '/YourLibrary_tracks.json')

df_library['UniqueID'] = df_library['artist'] + ":" + df_library['track']

new = df_library["uri"].str.split(":", expand = True)
df_library['track_uri'] = new[2]

df_library.head()

##

df_tableau = df_stream.copy()

df_tableau['In Library'] = np.where(df_tableau['UniqueID'].isin(df_library['UniqueID'].tolist()),1,0)

df_tableau = pd.merge(df_tableau, df_library[['album','UniqueID','track_uri']],how='left',on=['UniqueID'])

df_tableau.head()

##

CONFIG = json.load(open(config_filde, 'r'))
AUTH_URL = CONFIG['AUTH_URL']
SPOTIFY_CLIENT_ID = CONFIG['SPOTIFY_CLIENT_ID']
SPOTIFY_CLIENT_SECRET = CONFIG['SPOTIFY_CLIENT_SECRET']

auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': SPOTIFY_CLIENT_ID,
    'client_secret': SPOTIFY_CLIENT_SECRET,
})
auth_response_data = auth_response.json()

access_token = auth_response_data['access_token']

headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}
BASE_URL = 'https://api.spotify.com/v1/'
dict_all = {}

def do_spotify_request(url, headers, params = None):
    if params:
        r = requests.get(url, headers=headers, params = params)
    else:
        r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()

df_tableau = df_tableau.reset_index()
for row in df_tableau.iterrows():
    try:
        index = row[0]
        stream = row[1]
        t_uri = stream['track_uri']
        track_name = stream['trackName']
        artist_name = stream['artistName']

        # if uri is nan
        if type(t_uri) is float:
            params = [
                ('q', artist_name + ' track:' + track_name.replace('\'', ' ')),
                ('type', 'track'),
                ('market', 'FR'),
                ('limit', '1'),
                ('offset', '0')
                ]
            g = do_spotify_request(BASE_URL + 'search/', headers = headers, params = params)
            result_track = g['tracks']['items'][0]

            t_uri = result_track['uri'].split(':')[2]
            a_uri = result_track['artists'][0]['uri'].split(':')[2]
            track_popularity = result_track.get('popularity', None)
            track_duration_ms = result_track.get('duration_ms', None)
        else:
            r = do_spotify_request(BASE_URL + 'tracks/' + t_uri, headers = headers, params = params)
            a_uri = r['artists'][0]['uri'].split(':')[2]
            track_popularity = r.get('popularity', None)
            track_duration_ms = r.get('duration_ms', None)

        s = do_spotify_request(BASE_URL + 'artists/' + a_uri, headers = headers)
        artist_genres = s.get('genres', None)
        artist_popularity = s.get('popularity', None)

        f = do_spotify_request(BASE_URL + 'audio-features/' + t_uri, headers = headers)
        track_features = {
            'danceability': f.get('danceability', None),
            'energy': f.get('energy', None),
            'key': f.get('key', None),
            'loudness': f.get('loudness', None),
            'mode': f.get('mode', None),
            'speechiness': f.get('speechiness', None),
            'acousticness': f.get('acousticness', None),
            'instrumentalness': f.get('instrumentalness', None),
            'liveness': f.get('liveness', None),
            'valence': f.get('valence', None),
            'tempo': f.get('tempo', None)
        }
        
        dict_all[index] = {
            'end_time': stream['endTime'],
            'ms_played': stream['msPlayed'],
            'track': {
                'name': stream['trackName'],
                'uri': t_uri,
                'popularity': track_popularity,
                'duration': track_duration_ms,
                'features': track_features
            }, 
            #? todo all artitS
            'artist': {
                'name': stream['artistName'],
                'uri': a_uri,
                'popularity': artist_popularity,
                'genres': artist_genres
            },
            'album': {
                'name': stream['album']
            },
            'in_library': stream['In Library']
        }

    except requests.exceptions.HTTPError as err:
        print(f"WARN - HTTPError {index} - {err}")
        break
    except Exception as err:
        print(f"WARN - Exception {index} - {err}")

dict_all = pd.DataFrame.from_dict(dict_all, orient='index')
dict_all.reset_index(inplace=True, drop=True)

dict_all.head()

##

if not os.path.exists(results_folder):
    os.mkdir(results_folder)

dict_all.to_csv(results_folder + '/NewAllTable.csv')

print('done')
