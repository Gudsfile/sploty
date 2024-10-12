import logging

import pandas as pd

logger = logging.getLogger(__name__)


def main(input_paths: list, concated_path: str):
    """
    input_paths: files where read streaming history
    concated_path: file to write concated streaming history
    """
    # Read streaming files
    df_stream = pd.concat(map(pd.read_json, input_paths))
    logger.info("%i rows in %s", len(df_stream), input_paths)

    df_stream = df_stream.drop_duplicates()
    logger.info("%i rows without duplicated rows", len(df_stream))

    df_stream = df_stream.rename(
        columns={
            "ts": "end_time",
            "master_metadata_track_name": "track_name",
            "master_metadata_album_artist_name": "artist_name",
            "master_metadata_album_album_name": "album_name",
        },
    )
    df_stream["track_uri"] = df_stream["spotify_track_uri"].str.split(":", n=3, expand=True)[2]
    df_stream = df_stream.drop(
        ["spotify_track_uri", "episode_name", "episode_show_name", "spotify_episode_uri"],
        axis=1,
    )

    df_stream["id"] = df_stream.end_time + ":" + df_stream.track_uri

    df_stream["date"] = pd.to_datetime(df_stream.end_time)
    df_stream = df_stream.sort_values("date").reset_index(drop=True).drop("date", axis=1)

    # Save stream history
    df_stream.to_csv(concated_path, mode="w", index=False)
    logger.info("%i rows are saved at %s", len(df_stream), concated_path)
