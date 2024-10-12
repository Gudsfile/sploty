import logging
import time
from http import HTTPStatus
from itertools import batched

import pandas as pd
import requests
from pydantic import BaseModel, HttpUrl
from requests.exceptions import ConnectionError, HTTPError
from tinydb import TinyDB

logger = logging.getLogger(__name__)


class SpotifyApiParams(BaseModel):
    base_url: str
    endpoint: str
    url: HttpUrl
    headers: dict
    timeout: float
    sleep: float
    connection_error_retry: int = 5


def do_spotify_request(spotify_api_params: SpotifyApiParams, params=None, retry=0):
    try:
        response = requests.get(
            spotify_api_params.url,
            headers=spotify_api_params.headers,
            params=params,
            timeout=spotify_api_params.timeout,
        )
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
            logger.warning("HTTPError - %s (sleeping %is...)", err, spotify_api_params.sleep)
            time.sleep(spotify_api_params.sleep)
            return do_spotify_request(spotify_api_params, params)
        logger.warning("HTTPError - %s (skipping)", err)
        raise
    except ConnectionError as err:
        if retry < spotify_api_params.connection_error_retry:
            logger.warning(
                "ConnectionError - %s (retry %i/5 sleeping %is...)",
                err,
                retry + 1,
                spotify_api_params.sleep,
            )
            time.sleep(spotify_api_params.sleep * min(retry + 1))
            return do_spotify_request(spotify_api_params, params, retry + 1)
        logger.warning("ConnectionError - %s (skipping)", err)
        raise


def get_track_audio_features(spotify_api_params: SpotifyApiParams, track_uris):
    params = [("ids", ",".join(track_uris)), ("type", "track"), ("market", "FR")]
    response = do_spotify_request(spotify_api_params, params=params)
    return response["audio_features"]


def inserts_in_db(db, data):
    db.insert_multiple(data)


def inserts_enriched_tracks(db, tracks_uri, chunk_size, spotify_api_params):
    for chunk in batched(tracks_uri, chunk_size):
        track_audio_features = get_track_audio_features(spotify_api_params, chunk)
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


def main(  # noqa: PLR0913
    to_enrich_path: list,
    enriched_path: str,
    featured_path: str,
    chunk_size: int,
    spotify_api_params: SpotifyApiParams,
    db: TinyDB,
):
    # get the audio features of tracks saved it in the TinyDb
    df_stream = pd.read_csv(to_enrich_path)
    logger.info("%i streams", len(df_stream))

    track_uris_from_file = set(df_stream["track_uri"])
    track_uris_from_db = [track["id"] for track in db.all()]
    audio_feature_uris_to_get = track_uris_from_file.difference(track_uris_from_db)

    logger.info("%i unique uris", len(track_uris_from_file))
    logger.info("%i uris for which the audio features are already retrieved", len(track_uris_from_db))
    logger.info("%i uris for which the audio features must be recovered", len(audio_feature_uris_to_get))

    inserts_enriched_tracks(db, audio_feature_uris_to_get, chunk_size, spotify_api_params)

    track_uris_from_db = [track["id"] for track in db.all()]
    track_uris_failed_to_get = audio_feature_uris_to_get.difference(track_uris_from_db)
    logger.info("%i uris for which the audio features are now retrieved", len(track_uris_from_db))
    logger.info("%i uris for which the audio features could not be recovered", len(track_uris_failed_to_get))

    # add audio feature to rows that do not have it
    df_enriched_streams = pd.read_csv(enriched_path)
    df_audio_features = pd.DataFrame(db.all()).drop(
        ["type", "uri", "track_href", "analysis_url", "duration_ms"],
        axis=1,
    )

    df_completed_streams = completes_streams_with_audio_features(
        df_enriched_streams,
        "track_uri",
        df_audio_features,
        "id",
    )
    df_completed_streams.to_csv(featured_path, mode="w", index=False)
    logger.info(
        "%i rows are re-saved at %s with audio features completed",
        len(df_completed_streams),
        enriched_path,
    )


def get_spotify_auth_header(auth_url, client_id, client_secret, timeout):
    auth_response = requests.post(
        auth_url,
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=timeout,
    )
    auth_response_data = auth_response.json()
    return {"Authorization": f"Bearer {auth_response_data['access_token']}"}


def get_db(path):
    return TinyDB(path)
