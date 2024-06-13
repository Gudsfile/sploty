import glob
import json

import pandas as pd
from pandas import DatetimeIndex

CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r", encoding="utf8") as file:
    CONFIG = json.load(file.read())

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]

YOUR_STREAMING_HISTORY_FILES = "Streaming_History_Audio_*.json"
YOUR_STREAMING_HISTORY_PATHS = list(glob.glob(RESOURCES_FOLDER + "/" + YOUR_STREAMING_HISTORY_FILES))

ALL_YOUR_STREAMING_HISTORY_FILE = "AllStreamingHistory.csv"
ALL_YOUR_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + "/" + ALL_YOUR_STREAMING_HISTORY_FILE


def header_converter(df):
    return df.rename(columns={"endTime": "end_time", "msPlayed": "ms_played", "artistName": "artist_name", "trackName": "track_name"})


# Read streaming files
df_stream = header_converter(pd.concat(map(pd.read_json, YOUR_STREAMING_HISTORY_PATHS)))
print(f"INFO - {len(df_stream)} rows in {YOUR_STREAMING_HISTORY_PATHS}")

df_stream = df_stream.drop_duplicates()
print(f"INFO - {len(df_stream)} rows without duplicated rows")

df_stream = df_stream.rename(
    columns={
        "ts": "end_time",
        "master_metadata_track_name": "track_name",
        "master_metadata_album_artist_name": "artist_name",
        "master_metadata_album_album_name": "album_name",
    }
)
df_stream["track_uri"] = df_stream["spotify_track_uri"].str.split(":", n=3, expand=True)[2]
df_stream = df_stream.drop(["spotify_track_uri", "episode_name", "episode_show_name", "spotify_episode_uri"], axis=1)

df_stream["track_src_id"] = df_stream.artist_name + ":" + df_stream.track_name
df_stream["year"] = DatetimeIndex(df_stream.end_time).year.map(lambda x: f"{x:0>4}")  # pylint: disable=E1101
df_stream["month"] = (DatetimeIndex(df_stream.end_time).month).map(lambda x: f"{x:0>2}")  # pylint: disable=E1101
df_stream["month_name"] = DatetimeIndex(df_stream.end_time).month_name()  # pylint: disable=E1101
df_stream["day"] = DatetimeIndex(df_stream.end_time).day.map(lambda x: f"{x:0>2}")  # pylint: disable=E1101
df_stream["hour"] = DatetimeIndex(df_stream.end_time).hour.map(lambda x: f"{x:0>2}")  # pylint: disable=E1101
df_stream["minute"] = DatetimeIndex(df_stream.end_time).minute.map(lambda x: f"{x:0>2}")  # pylint: disable=E1101
# ":04" writting is fixed in Python 3.10+ : https://stackoverflow.com/a/36044788

df_stream["min_played"] = df_stream.ms_played / 1000 / 60
df_stream["id"] = df_stream.end_time + ":" + df_stream.track_uri

df_stream["date"] = pd.to_datetime(df_stream.end_time)
df_stream = df_stream.sort_values("date").reset_index(drop=True).drop("date", axis=1)

# Save stream history
df_stream.to_csv(ALL_YOUR_STREAMING_HISTORY_PATH, mode="w", index=False)
print(f"INFO - {len(df_stream)} rows are saved at {ALL_YOUR_STREAMING_HISTORY_PATH}")
