import json
import time
from http import HTTPStatus
from itertools import batched
from pathlib import Path

import pandas as pd
import requests
from requests.exceptions import HTTPError
from settings import logger
from tinydb import TinyDB

CONFIG_FILE = "config.json"
with Path(CONFIG_FILE).open(encoding="utf8") as file:
    CONFIG = json.load(file)

CHUNK_SIZE = CONFIG["file"]["chunk_size"]

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE = "AllStreamingHistoryToEnrich.csv"
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH = RESOURCES_FOLDER + "/" + ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE

# Some rows are already enriched with audio features, some aren't
YOUR_ENRICHED_STREAMING_HISTORY_FILE = "AllEnrichedStreamingHistory.csv"
YOUR_ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + "/" + YOUR_ENRICHED_STREAMING_HISTORY_FILE

# Spotify's authentication and config
SPOTIFY_CLIENT_ID = CONFIG["spotify"]["client_id"]
SPOTIFY_CLIENT_SECRET = CONFIG["spotify"]["client_secret"]
SPOTIFY_AUTH_URL = CONFIG["spotify"]["auth_url"]
SPOTIFY_BASE_URL = CONFIG["spotify"]["base_url"]
SPOTIFY_SLEEP = CONFIG["spotify"]["s_sleep"]
SPOTIFY_TIMEOUT = CONFIG["spotify"]["timeout"]

auth_response = requests.post(
    SPOTIFY_AUTH_URL,
    {
        "grant_type": "client_credentials",
        "client_id": SPOTIFY_CLIENT_ID,
        "client_secret": SPOTIFY_CLIENT_SECRET,
    },
    timeout=SPOTIFY_TIMEOUT,
)
auth_response_data = auth_response.json()

ACCESS_TOKEN = auth_response_data["access_token"]
SPOTIFY_HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

# TinyDb
DB_PATH = CONFIG["tinydb"]["track_audio_features"]
DB = TinyDB(DB_PATH)


def do_spotify_request(url, headers, params=None):
    try:
        response = requests.get(url, headers=headers, params=params, timeout=120)
        logger.debug(" -> %s", response.request.url)
        logger.debug(
            " <- %i %s",
            response.status_code,
            response.text[:200].replace(" ", "").encode("UTF-8"),
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as err:
        if err.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            logger.warning("HTTPError - %s (sleeping %is...)", err, SPOTIFY_SLEEP)
            time.sleep(SPOTIFY_SLEEP)
            return do_spotify_request(url, headers, params)
        logger.warning("HTTPError - %s (skipping)", err)
        raise


def get_track_audio_features(track_uris: list):
    params = [("ids", ",".join(track_uris)), ("type", "track"), ("market", "FR")]
    response = do_spotify_request(SPOTIFY_BASE_URL + "audio-features/", headers=SPOTIFY_HEADERS, params=params)
    return response["audio_features"]


def inserts_in_db(db, data):
    db.insert_multiple(data)


def inserts_enriched_tracks(db, tracks_uri, chunk_size):
    for chunk in batched(tracks_uri, chunk_size):
        track_audio_features = get_track_audio_features(chunk)
        track_audio_features_without_none_value = [taf for taf in track_audio_features if taf]
        inserts_in_db(db, track_audio_features_without_none_value)


def completes_streams_with_audio_features(df_left, left_key, df_right, right_key):
    df_completed = df_left.merge(df_right, left_on=left_key, right_on=right_key, how="left")
    # values initial col with new one if it is na
    for col_x in list(filter(lambda x: x.endswith("_x"), df_completed.columns.to_list())):
        col = col_x[:-2]
        col_y = col_x[:-1] + "y"
        df_completed[col] = df_completed[col_x].fillna(df_completed[col_y])
        df_completed = df_completed.drop([col_x, col_y], axis=1)
    return df_completed


# TODO clean this part
# this part is to drop duplicate uris in the tinydb
# maliste=['A', 'B', 'A', 'C', 'E', 'E', 'E']#noqa: ERA001
# def get_dup_id(maliste):
#     monres = list()#noqa: ERA001
#     for i in range(1, len(maliste)):
#         if maliste[i] in maliste[:i]:
#             monres.append(i)#noqa: ERA001
#     return monres#noqa: ERA001
#
# dup_id = get_dup_id(maliste)#noqa: ERA001
# db.remove(doc_ids=dup_id)#noqa: ERA001
# len(db.all())#noqa: ERA001

# get the audio features of tracks saved it in the TinyDb
df_stream = pd.read_csv(ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH)
logger.info("%i streams", len(df_stream))

track_uris_from_file = set(df_stream["track_uri"])
track_uris_from_db = [track["id"] for track in DB.all()]
audio_feature_uris_to_get = track_uris_from_file.difference(track_uris_from_db)

logger.info("%i unique uris", len(track_uris_from_file))
logger.info("%i uris for which the audio features are already retrieved", len(track_uris_from_db))
logger.info("%i uris for which the audio features must be recovered", len(audio_feature_uris_to_get))

inserts_enriched_tracks(DB, audio_feature_uris_to_get, CHUNK_SIZE)

track_uris_from_db = [track["id"] for track in DB.all()]
track_uris_failed_to_get = audio_feature_uris_to_get.difference(track_uris_from_db)
logger.info("%i uris for which the audio features are now retrieved", len(track_uris_from_db))
logger.info("%i uris for which the audio features could not be recovered", len(track_uris_failed_to_get))

# add audio feature to rows that do not have it
df_enriched_streams = pd.read_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH)
df_audio_features = pd.DataFrame(DB.all())

df_completed_streams = completes_streams_with_audio_features(df_enriched_streams, "track_uri", df_audio_features, "id")
df_completed_streams.to_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH, mode="w", index=False)
logger.info(
    "%i rows are re-saved at %s with audio features completed",
    len(df_completed_streams),
    YOUR_ENRICHED_STREAMING_HISTORY_PATH,
)
