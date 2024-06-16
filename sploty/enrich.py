import json
import os
import time
from enum import Enum

import numpy as np
import pandas as pd
import requests
from requests.exceptions import HTTPError

CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r", encoding="utf8") as file:
    CONFIG = json.load(file.read())

CHUNK_SIZE = CONFIG["file"]["chunk_size"]

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE = "AllStreamingHistoryToEnrich.csv"
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH = RESOURCES_FOLDER + "/" + ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE

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


# Color
class BoldColor(str, Enum):
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    DARKCYAN = "\033[36m"
    BLUE = "\033[94m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"


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
        response = requests.get(url, headers=headers, params=params, timeout=SPOTIFY_TIMEOUT)
        print(f" -> {response.request.url}")
        print(f" <- {response.status_code} {response.text[:200].replace(' ', '').encode('UTF-8')}")
        response.raise_for_status()
        return response.json()
    except HTTPError as err:
        if err.response.status_code == 429:
            print(f"WARN - HTTPError - {err} (sleeping {SPOTIFY_SLEEP}s...)")
            time.sleep(SPOTIFY_SLEEP)
            return do_spotify_request(url, headers, params)
        print(f"WARN - HTTPError - {err} (skipping)")
        raise


def another_get(track_uris):
    params = [("ids", ",".join(track_uris)), ("type", "track"), ("market", "FR")]
    return do_spotify_request(SPOTIFY_BASE_URL + "tracks/", headers=SPOTIFY_HEADERS, params=params)


def merger(df1, df5):
    df1["is_done"] = df1.track_src_id.isin(df5.track_src_id)
    df = df1.merge(df5, on="track_src_id", how="left")

    for c_x in df.columns:
        if c_x.endswith("_x"):
            c = c_x.removesuffix("_x")
            c_y = c + "_y"
            df[c] = df[c_y].where(df.is_done, df[c_x])
    return df.loc[:, ~df.columns.str.contains("_x$|_y$")]


def saver(df_tableau, complete_data):
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
    to_write = streams[streams["is_done"] == True][sorted_cols]  # pylint: disable=C0121
    to_keep = streams[streams["is_done"] == False]  # pylint: disable=C0121
    # == to prevent "KeyError: False"

    # writes data in csv file
    to_write.to_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH, mode="a", header=not os.path.exists(YOUR_ENRICHED_STREAMING_HISTORY_PATH), index=False)

    return to_keep.reset_index(drop=True)


def better_enrich(df_tableau):
    print(f"INFO - enrich track data for {len(df_tableau)} tracks")

    df = df_tableau[["track_uri", "track_name", "artist_name", "track_src_id", "ms_played"]].drop_duplicates("track_src_id")
    print(f"INFO - reduce enrich for only {len(df)} tracks")

    dict_all = {}
    target = len(df)
    step = CHUNK_SIZE * 10
    checkpoint = 0
    for rows in chunks_iter(df.iterrows(), CHUNK_SIZE):
        print(
            " " * 40
            + BoldColor.PURPLE
            + "["
            + "-" * int(checkpoint / step)
            + " " * int((target - checkpoint) / step)
            + "]"
            + BoldColor.DARKCYAN
            + f" {checkpoint}/{target}"
            + BoldColor.END
        )
        # print(f'{" "*40}{BoldColor.PURPLE}[{"-"*int(checkpoint / step)}{" "* int((target - checkpoint) / step)}]{BoldColor.DARKCYAN} {checkpoint}/{target}{BoldColor.END}')
        response = another_get([row[1]["track_uri"] for row in rows])  # il doit y avoir mieux
        for i, row in enumerate(rows):
            index = row[0]
            stream = row[1]

            track = response["tracks"][i]
            artist = track["artists"][0]  # only one artist :(
            # album = track["album"]
            track_uri = track["uri"]

            print(f"DEBUG - enrich track uri n°{index} ({track_uri})")

            stream["artist_uri"] = artist["uri"].split(":")[2]
            stream["track_duration_ms"] = track.get("duration_ms", np.nan) if track.get("duration_ms", None) != 0.0 else np.nan
            stream["track_popularity"] = track.get("popularity", None)
            stream["percentage_played"] = round((stream.ms_played / stream.track_duration_ms) * 100, 2)
            dict_all[index] = stream
        checkpoint += CHUNK_SIZE
        df_tableau = saver(df_tableau, dict_all)
        dict_all = {}


def number_of_lines(file_path: str):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="UTF-8") as f:
            return sum(1 for _ in f)
    return 0


# enriches the data tracks and indexes it
df_stream = pd.read_csv(ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH)
print(f"INFO - {len(df_stream)} rows to enrich")

old_number_of_enriched_streams = number_of_lines(YOUR_ENRICHED_STREAMING_HISTORY_PATH)

better_enrich(df_stream)

new_number_of_enriched_streams = number_of_lines(YOUR_ENRICHED_STREAMING_HISTORY_PATH)
print(f"INFO - {new_number_of_enriched_streams-old_number_of_enriched_streams} tracks enriched / {len(df_stream)} rows to enrich")
