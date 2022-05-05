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
dict_genre = {}
track_uris = df_library['track_uri'].to_list()

for t_uri in track_uris:
    try:
        dict_genre[t_uri] = {'artist_uri': "", "genres":[]}
        
        r = requests.get(BASE_URL + 'tracks/' + t_uri, headers=headers)
        r = r.json()
        a_uri = r['artists'][0]['uri'].split(':')[2]
        dict_genre[t_uri]['artist_uri'] = a_uri
        
        s = requests.get(BASE_URL + 'artists/' + a_uri, headers=headers)
        s = s.json()
        dict_genre[t_uri]['genres'] = s['genres']
    except Exception as err:
        print(f"WARN - {err}")

df_genre = pd.DataFrame.from_dict(dict_genre, orient='index')
df_genre.insert(0, 'track_uri', df_genre.index)
df_genre.reset_index(inplace=True, drop=True)

df_genre.head()

df_genre_expanded = df_genre.explode('genres')
df_genre_expanded.head()

##

if not os.path.exists(results_folder):
    os.mkdir(results_folder)

df_tableau.to_csv(results_folder + '/MySpotifyDataTable.csv')
df_genre_expanded.to_csv(results_folder + '/GenresExpandedTable.csv')

print('done')
