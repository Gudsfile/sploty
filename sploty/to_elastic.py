import json
from pathlib import Path

import pandas as pd
from elasticsearch import Elasticsearch, helpers
from settings import logger

CONFIG_FILE = "config.json"

with Path(CONFIG_FILE).open(encoding="utf8") as file:
    CONFIG = json.load(file)

CHUNK_SIZE = CONFIG["file"]["chunk_size"]

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]
YOUR_ENRICHED_STREAMING_HISTORY_FILE = "AllEnrichedStreamingHistory.csv"
YOUR_ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + "/" + YOUR_ENRICHED_STREAMING_HISTORY_FILE

# Elastic authentication and config
ELASTIC_HOSTS = CONFIG["elasticsearch"]["hosts"]
ELASTIC_AUTH = (CONFIG["elasticsearch"]["username"], CONFIG["elasticsearch"]["password"])
ELASTIC_INDEX_NAME = CONFIG["elasticsearch"]["index"]["name"]
ELASTIC = Elasticsearch(hosts=ELASTIC_HOSTS, basic_auth=ELASTIC_AUTH)
ELASTIC.options(request_timeout=CONFIG["elasticsearch"]["request_timeout"])


def bulk_factory(df, index_name):
    for document in df:
        yield {"_index": index_name, "_id": document.pop("id"), "_source": document}


def set_multidata(elastic, data, index_name):
    logger.debug(" -> bulk %i documents", len(data))
    response = helpers.bulk(elastic, bulk_factory(data, index_name), raise_on_error=False)
    logger.debug(" <- bulk response is %s", response)


# Read enriched streams
df_stream = pd.read_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH)

# Rename columns
df_stream = df_stream.rename(
    columns={
        "end_time": "end_time",
        "ms_played": "ms_played",
        "min_played": "min_played",
        "percentage_played": "percentage_played",
        "track_uri": "track_uri",
        "track_name": "track_name",
        "track_duration_ms": "track_duration_ms",
        "track_popularity": "track_popularity",
        "track_is_in_library": "track_is_in_library",
        "track_is_unplayable": "track_is_unplayable",
        "artist_uri": "artist_uri",
        "artist_name": "artist_name",
        "artist_genres": "artist_genres",
        "artist_popularity": "artist_popularity",
        "album_uri": "album_uri",
        "audio_features": "audio_features",
        "stream_context": "stream_context",
        "username": "stream_username",
        "platform": "stream_platform",
        "conn_country": "stream_conn_country",
        "ip_addr_decrypted": "stream_ip_addr_decrypted",
        "user_agent_decrypted": "stream_user_agent_decrypted",
        "album_name": "stream_album_name",
        "reason_start": "stream_reason_start",
        "reason_end": "stream_reason_end",
        "shuffle": "stream_shuffle",
        "skipped": "stream_skipped",
        "offline": "stream_offline",
        "offline_timestamp": "stream_offline_timestamp",
        "incognito_mode": "stream_incognito_mode",
        "month_name": "month_name",
        "id": "id",
        "danceability": "track_audio_feature_danceability",
        "energy": "track_audio_feature_energy",
        "key": "track_audio_feature_key",
        "loudness": "track_audio_feature_loudness",
        "mode": "track_audio_feature_mode",
        "speechiness": "track_audio_feature_speechiness",
        "acousticness": "track_audio_feature_acousticness",
        "instrumentalness": "track_audio_feature_instrumentalness",
        "liveness": "track_audio_feature_liveness",
        "valence": "track_audio_feature_valence",
        "tempo": "track_audio_feature_tempo",
        "time_signature": "track_audio_feature_time_signature",
    },
)

df_stream = df_stream.drop(["track_src_id"], axis=1)
df_stream = df_stream.drop(["minute", "hour", "day", "month", "year"], axis=1)
df_stream = df_stream.drop(["stream_skipped"], axis=1)  # TODO fix error

# Index streams
logger.info("indexing %i tracks to %s", len(df_stream), ELASTIC_INDEX_NAME)
json_tmp = json.loads(df_stream.to_json(orient="records"))
logger.debug("%s", json_tmp[-1])
set_multidata(ELASTIC, json_tmp, ELASTIC_INDEX_NAME)
set_multidata(ELASTIC, json_tmp, ELASTIC_INDEX_NAME)
