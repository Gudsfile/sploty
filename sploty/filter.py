import glob
import json

import pandas as pd
from pandas.errors import EmptyDataError

CONFIG_FILE = "config.json"
with open(CONFIG_FILE, "r", encoding="utf8") as file:
    CONFIG = json.load(file.read())

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]

YOUR_STREAMING_HISTORY_FILES = "StreamingHistory*.json"
YOUR_STREAMING_HISTORY_PATHS = list(glob.glob(RESOURCES_FOLDER + "/" + YOUR_STREAMING_HISTORY_FILES))

ALL_YOUR_STREAMING_HISTORY_FILE = "AllStreamingHistory.csv"
ALL_YOUR_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + "/" + ALL_YOUR_STREAMING_HISTORY_FILE

ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE = "AllStreamingHistoryToEnrich.csv"
ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH = RESOURCES_FOLDER + "/" + ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_FILE

YOUR_ENRICHED_STREAMING_HISTORY_FILE = "AllEnrichedStreamingHistory.csv"
YOUR_ENRICHED_STREAMING_HISTORY_PATH = RESOURCES_FOLDER + "/" + YOUR_ENRICHED_STREAMING_HISTORY_FILE

# Read track history Spotify file
df_stream = pd.read_csv(ALL_YOUR_STREAMING_HISTORY_PATH)
print(f"INFO - {len(df_stream)} rows to enrich")

try:
    df_already_enriched = pd.read_csv(YOUR_ENRICHED_STREAMING_HISTORY_PATH)
    print(f"INFO - {len(df_already_enriched)} enriched rows found")
    # TODO there is an error : it lost new stream of a previously enriched track
    # df_stream = df_stream[(~df_stream.track_uri.isin(df_already_enriched.track_uri)) | (df_stream.end_time > max(df_already_enriched.end_time))]
    # df_stream = df_stream[(~df_stream.id.isin(df_already_enriched.id))]
    df_stream = df_stream[df_stream.end_time > max(df_already_enriched.end_time)]
    print(f"INFO - only {len(df_stream)} rows to enrich")
except EmptyDataError:
    print(f"WARN - empty backup file found ({YOUR_ENRICHED_STREAMING_HISTORY_PATH}")
except FileNotFoundError:
    print(f"WARN - no backup file found ({YOUR_ENRICHED_STREAMING_HISTORY_PATH}")

# Drop NaN row
df_stream = df_stream[df_stream["track_uri"].notna()]
print(f"INFO - {len(df_stream)} rows to enrich without empty track_uri")

# Save stream to enrich
df_stream.to_csv(ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH, mode="w", index=False)
print(f"INFO - {len(df_stream)} rows are saved at {ALL_YOUR_STREAMING_HISTORY_TO_ENRICH_PATH}")
