import json
import os

import pandas as pd
import numpy as np
import requests

# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html
# https://towardsdatascience.com/get-your-spotify-streaming-history-with-python-d5a208bbcbd3

index = '3'

resources_folder = 'resources/my_spotify_data_' + index
results_folder = 'results/my_spotify_data_' + index

config_filde = 'config.json'

##

df_stream0 = pd.read_json(resources_folder + '/StreamingHistory0.json')
df_stream1 = pd.read_json(resources_folder + '/StreamingHistory1.json')
df_stream2 = pd.read_json(resources_folder + '/StreamingHistory2.json')
df_stream3 = pd.read_json(resources_folder + '/StreamingHistory3.json')

df_stream = pd.concat([df_stream0, df_stream1, df_stream2])

df_stream['UniqueID'] = df_stream['artistName'] + ':' + df_stream['trackName']

df_stream.head()

##

df_library = pd.read_json(resources_folder + '/YourLibrary_tracks.json')

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
            g = requests.get(BASE_URL + 'search/', headers=headers, params = params)
            g = g.json()
            result_track = g['tracks']['items'][0]

            t_uri = result_track['uri'].split(':')[2]
            a_uri = result_track['artists'][0]['uri'].split(':')[2]
            track_popularity = result_track['popularity']
            track_duration_ms = result_track['duration_ms']
        else:
            r = requests.get(BASE_URL + 'tracks/' + t_uri, headers=headers)
            r = r.json()
            a_uri = r['artists'][0]['uri'].split(':')[2]
            track_popularity = r['popularity']
            track_duration_ms = r['duration_ms']

        s = requests.get(BASE_URL + 'artists/' + a_uri, headers=headers)
        s = s.json()
        artist_genres = s['genres']
        artist_popularity = s['popularity']

        f = requests.get(BASE_URL + 'audio-features/' + t_uri, headers=headers)
        f = f.json()
        track_features = {
            'danceability': f['danceability'],
            'energy': f['energy'],
            'key': f['key'],
            'loudness': f['loudness'],
            'mode': f['mode'],
            'speechiness': f['speechiness'],
            'acousticness': f['acousticness'],
            'instrumentalness': f['instrumentalness'],
            'liveness': f['liveness'],
            'valence': f['valence'],
            'tempo': f['tempo']          
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

    except Exception as err:
        print(f"WARN - {err}")

dict_all = pd.DataFrame.from_dict(dict_all, orient='index')
dict_all.reset_index(inplace=True, drop=True)

dict_all.head()

##

if not os.path.exists(results_folder):
    os.mkdir(results_folder)

dict_all.to_csv(results_folder + '/NewAllTable.csv')

print('done')
