import json
import logging

import pandas as pd
from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)


def bulk_factory(df, index_name):
    for document in df:
        yield {"_index": index_name, "_id": document.pop("id"), "_source": document}


def set_multidata(elastic, data, index_name):
    logger.debug(" -> bulk %i documents", len(data))
    response = helpers.bulk(elastic, bulk_factory(data, index_name), raise_on_error=False)
    logger.debug(" <- bulk response is %s", response)


def main(enriched_path: str, index_name: str, elastic):
    # Read enriched streams
    df_stream = pd.read_csv(enriched_path)

    # Rename columns
    df_stream = df_stream.rename(
        columns={
            "username": "stream_username",
            "platform": "stream_platform",
            "normalized_platform": "stream_normalized_platform",
            "conn_country": "stream_conn_country",
            "ip_addr_decrypted": "stream_ip_addr_decrypted",
            "user_agent_decrypted": "stream_user_agent_decrypted",
            "reason_start": "stream_reason_start",
            "reason_end": "stream_reason_end",
            "shuffle": "stream_shuffle",
            "skipped": "stream_skipped",
            "offline": "stream_offline",
            "offline_timestamp": "stream_offline_timestamp",
            "incognito_mode": "stream_incognito_mode",
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

    # Index streams
    logger.info("indexing %i tracks to %s", len(df_stream), index_name)
    json_tmp = json.loads(df_stream.to_json(orient="records"))
    logger.debug("%s", json_tmp[-1])
    set_multidata(elastic, json_tmp, index_name)


def get_elastic(hosts, username, password, timeout):
    elastic = Elasticsearch(hosts=hosts, basic_auth=(username, password))
    elastic.options(request_timeout=timeout)
    return elastic
