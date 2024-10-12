import logging
from pathlib import Path

import pandas as pd
from pandas import DatetimeIndex

logger = logging.getLogger(__name__)


def normalize_platform(platform: str):
    normalized = {
        "android os": "Android OS",
        "android [arm 0]": "Android OS",
        "android-tablet os": "Android OS",
        "partner android_tv": "Android TV",
        "partner google cast": "Chromecast",
        "ios": "iOS",
        "partner ios": "iOS",
        "osx": "MacOS",
        "os x": "MacOS",
        "sonos_": "Sonos",
        "partner sonos": "Sonos",
        "webos tv": "WebOS TV",
        "partner webos_tv": "WebOS tv",
        "webplayer": "WebPlayer",
        "web_player": "WebPlayer",
        "partner spotify web_player": "WebPlayer",
        "windows": "Windows",
        "not_applicable": "not_applicable",
    }
    normalized_matches = [value for key, value in normalized.items() if platform.lower().startswith(key.lower())]
    if len(normalized_matches) > 1:
        logger.warning(
            "There are several matches for the `%s` platform: %s (the first one is taken)",
            platform,
            normalized_matches,
        )
        return normalized_matches[0]
    if len(normalized_matches) < 1:
        logger.warning("There is no match for the `%s` platform", platform)
        return platform
    return normalized_matches[0]


def main(enriched_path: Path, metrics_path: Path):
    df_stream = pd.read_csv(enriched_path)

    df_stream["year"] = DatetimeIndex(df_stream.end_time).year.map(lambda x: f"{x:0>4}")
    df_stream["month"] = (DatetimeIndex(df_stream.end_time).month).map(lambda x: f"{x:0>2}")
    df_stream["month_name"] = DatetimeIndex(df_stream.end_time).month_name()
    df_stream["day"] = DatetimeIndex(df_stream.end_time).day.map(lambda x: f"{x:0>2}")
    df_stream["day_of_week"] = DatetimeIndex(df_stream.end_time).day_of_week.map(lambda x: f"{x:0>2}")
    df_stream["day_name"] = DatetimeIndex(df_stream.end_time).day_name()
    df_stream["hour"] = DatetimeIndex(df_stream.end_time).hour.map(lambda x: f"{x:0>2}")
    df_stream["minute"] = DatetimeIndex(df_stream.end_time).minute.map(lambda x: f"{x:0>2}")
    # ":04" writting is fixed in Python 3.10+ : https://stackoverflow.com/a/36044788

    df_stream["min_played"] = df_stream.ms_played / 1000 / 60

    df_stream["percentage_played"] = round((df_stream.ms_played / df_stream.track_duration_ms) * 100, 2)
    df_stream["percentage_played"] = df_stream["percentage_played"].clip(0, 100)

    df_stream["is_new_track"] = ~df_stream["track_uri"].duplicated(keep="first")
    df_stream["is_new_artist"] = ~df_stream["artist_uri"].duplicated(keep="first")
    df_stream["is_new_album"] = ~df_stream["album_uri"].duplicated(keep="first")

    df_stream["normalized_platform"] = df_stream["platform"].apply(normalize_platform)

    df_stream["skipped"] = df_stream["skipped"].astype(bool)

    df_stream.to_csv(metrics_path, mode="w", index=False)
