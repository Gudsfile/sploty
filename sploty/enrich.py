import json
import logging
import time
from http import HTTPStatus
from itertools import batched
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from pydantic import BaseModel, HttpUrl
from requests.exceptions import HTTPError
from settings import BoldColor

logger = logging.getLogger(__name__)


class SpotifyApiParams(BaseModel):
    base_url: str
    endpoint: str
    url: HttpUrl
    headers: dict
    timeout: float
    sleep: float


def do_spotify_request(spotify_api_params: SpotifyApiParams, params=None):
    try:
        response = requests.get(
            spotify_api_params.url,
            headers=spotify_api_params.headers,
            params=params,
            timeout=spotify_api_params.timeout,
        )
        logger.debug(" -> %s", response.request.url)
        logger.debug(" <- %i %s", response.status_code, response.text[:200].replace(" ", "").encode("UTF-8"))
        response.raise_for_status()
        return response.json()
    except HTTPError as err:
        if err.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            logger.warning("HTTPError - %s (sleeping %is...)", err, spotify_api_params.sleep)
            time.sleep(spotify_api_params.sleep)
            return do_spotify_request(spotify_api_params, params)
        logger.warning("HTTPError - %s (skipping)", err)
        raise


def another_get(spotify_api_params: SpotifyApiParams, track_uris):
    params = [("ids", ",".join(track_uris)), ("type", "track"), ("market", "FR")]
    return do_spotify_request(spotify_api_params, params=params)


def merger(df1, df5):
    df1["is_done"] = df1.track_src_id.isin(df5.track_src_id)
    df = df1.merge(df5, on="track_src_id", how="left")

    for c_x in df.columns:
        if c_x.endswith("_x"):
            c = c_x.removesuffix("_x")
            c_y = c + "_y"
            df[c] = df[c_y].where(df.is_done, df[c_x])
    return df.loc[:, ~df.columns.str.contains("_x$|_y$")]


def saver(df_tableau, complete_data, enriched_path):
    sorted_cols = [
        "id",
        "end_time",
        "artist_name",
        "track_name",
        "ms_played",
        "min_played",
        "track_duration_ms",
        "percentage_played",
        "track_popularity",
        # 'in_library',
        "track_src_id",
        "artist_uri",
        "track_uri",
        "year",
        "month",
        "month_name",
        "day",
        "hour",
        "minute",
        "username",
        "platform",
        "conn_country",
        "ip_addr_decrypted",
        "user_agent_decrypted",
        "album_name",
        "reason_start",
        "reason_end",
        "shuffle",
        "skipped",
        "offline",
        "offline_timestamp",
        "incognito_mode",
    ]

    complete_data = pd.DataFrame.from_dict(complete_data, orient="index")

    if len(complete_data) == 0:
        return df_tableau

    streams = merger(df_tableau, complete_data)
    to_write = streams[streams["is_done"] == True][sorted_cols]  # noqa: E712
    to_keep = streams[streams["is_done"] == False]  # noqa: E712
    # == to prevent "KeyError: False"

    # writes data in csv file
    to_write.to_csv(
        enriched_path,
        mode="a",
        header=not Path(enriched_path).exists(),
        index=False,
    )

    return to_keep.reset_index(drop=True)


def better_enrich(df_tableau, chunk_size, enriched_path, spotify_api_params):
    logger.info("enrich track data for %i tracks", len(df_tableau))

    df = df_tableau[["track_uri", "track_name", "artist_name", "track_src_id", "ms_played"]].drop_duplicates(
        "track_src_id",
    )
    logger.info("reduce enrich for only %i tracks", len(df))

    dict_all = {}
    target = len(df)
    checkpoint = 0
    for rows in batched(df.iterrows(), chunk_size):
        logger.info(
            BoldColor.PURPLE  # noqa: G003
            + "["
            + ("-" * int(checkpoint * 60 / target)).ljust(60, " ")
            + "]"
            + BoldColor.DARKCYAN
            + f" {checkpoint}/{target}"
            + BoldColor.END,
        )
        # logger.info(f'{" "*40}{BoldColor.PURPLE}[{"-"*int(checkpoint / step)}{" "* int((target - checkpoint) / step)}]{BoldColor.DARKCYAN} {checkpoint}/{target}{BoldColor.END}') #noqa: ERA001, E501
        response = another_get(spotify_api_params, [row[1]["track_uri"] for row in rows])  # il doit y avoir mieux
        for i, row in enumerate(rows):
            index = row[0]
            stream = row[1]

            track = response["tracks"][i]
            artist = track["artists"][0]  # only one artist :(
            # album = track["album"] #noqa: ERA001
            track_uri = track["uri"]

            logger.debug("enrich track uri n°%i (%s)", index, track_uri)

            stream["artist_uri"] = artist["uri"].split(":")[2]
            stream["track_duration_ms"] = (
                track.get("duration_ms", np.nan) if track.get("duration_ms", None) != 0.0 else np.nan
            )
            stream["track_popularity"] = track.get("popularity", None)
            stream["percentage_played"] = round((stream.ms_played / stream.track_duration_ms) * 100, 2)
            dict_all[index] = stream
        checkpoint += chunk_size
        df_tableau = saver(df_tableau, dict_all, enriched_path)
        dict_all = {}


def number_of_lines(file_path: str):
    fp = Path(file_path)
    if fp.exists():
        with fp.open(encoding="UTF-8") as file:
            return sum(1 for _ in file)
    return 0


def main(to_enrich_path: list, enriched_path: str, chunk_size: int, spotify_api_params: SpotifyApiParams):
    """
    to_enrich_path: file where read filtered streaming history
    enriched_path: file where already enriched streaming history
    chunk_size:
    """
    # enriches the data tracks and indexes it
    df_stream = pd.read_csv(to_enrich_path)
    logger.info("%i rows to enrich", len(df_stream))

    old_number_of_enriched_streams = number_of_lines(enriched_path)

    better_enrich(df_stream, chunk_size, enriched_path, spotify_api_params)

    new_number_of_enriched_streams = number_of_lines(enriched_path)
    logger.info(
        "%i tracks enriched / %i rows to enrich",
        new_number_of_enriched_streams - old_number_of_enriched_streams,
        len(df_stream),
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


if __name__ == "__main__":
    CONFIG_FILE = "config.json"
    with Path(CONFIG_FILE).open(encoding="utf8") as file:
        CONFIG = json.load(file)

    CHUNK_SIZE = CONFIG["file"]["chunk_size"]

    # Files
    RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]
    ALL_STREAMING_HISTORY_TO_ENRICH_FILE = "AllStreamingHistoryToEnrich.csv"
    ALL_STREAMING_HISTORY_TO_ENRICH_PATH = RESOURCES_FOLDER + "/" + ALL_STREAMING_HISTORY_TO_ENRICH_FILE

    ENRICHED_STREAMING_HISTORY_FILE = "AllEnrichedStreamingHistory.csv"
    ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + "/" + ENRICHED_STREAMING_HISTORY_FILE

    # Spotify's authentication and config
    SPOTIFY_CLIENT_ID = CONFIG["spotify"]["client_id"]
    SPOTIFY_CLIENT_SECRET = CONFIG["spotify"]["client_secret"]
    SPOTIFY_AUTH_URL = CONFIG["spotify"]["auth_url"]
    SPOTIFY_BASE_URL = CONFIG["spotify"]["base_url"]
    SPOTIFY_SLEEP = CONFIG["spotify"]["s_sleep"]
    SPOTIFY_TIMEOUT = CONFIG["spotify"]["timeout"]

    SPOTIFY_API_PARAMS = SpotifyApiParams(
        base_url=SPOTIFY_BASE_URL,
        endpoint="tracks",
        url=f"{SPOTIFY_BASE_URL}/tracks/",
        headers=get_spotify_auth_header(SPOTIFY_AUTH_URL, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_TIMEOUT),
        timeout=SPOTIFY_TIMEOUT,
        sleep=SPOTIFY_SLEEP,
    )

    main(ALL_STREAMING_HISTORY_TO_ENRICH_PATH, ENRICHED_STREAMING_HISTORY_PATH, CHUNK_SIZE, SPOTIFY_API_PARAMS)
