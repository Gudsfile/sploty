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

from sploty.settings import BoldColor

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
    df1["is_done"] = df1.track_uri.isin(df5.track_uri)
    df = df1.merge(df5, on="track_uri", how="left")

    for c_x in df.columns:
        if c_x.endswith("_x"):
            c = c_x.removesuffix("_x")
            c_y = c + "_y"
            df[c] = df[c_y].where(df.is_done, df[c_x])
    return df.loc[:, ~df.columns.str.contains("_x$|_y$")]


def saver(df_tableau, complete_data, enriched_path):
    complete_data = pd.DataFrame.from_dict(complete_data, orient="index")

    if len(complete_data) == 0:
        return df_tableau

    streams = merger(df_tableau, complete_data)
    to_write = streams[streams["is_done"] == True]  # noqa: E712
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

    df = df_tableau[["track_uri", "track_name", "artist_name", "ms_played"]].drop_duplicates(
        "track_uri",
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
            album = track["album"]
            track_uri = track["uri"]

            logger.debug("enrich track uri n°%i (%s)", index, track_uri)

            stream["artist_uri"] = artist["id"]
            stream["track_duration_ms"] = (
                track.get("duration_ms", np.nan) if track.get("duration_ms", None) != 0.0 else np.nan
            )
            stream["track_popularity"] = track.get("popularity", None)
            stream["track_is_explicit"] = track["explicit"]
            stream["track_is_local"] = track["is_local"]
            stream["track_is_playable"] = track["is_playable"]
            stream["album_uri"] = album["id"]
            stream["album_type"] = album["album_type"]
            stream["album_release_date"] = album["release_date"]
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
