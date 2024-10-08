from __future__ import annotations

import logging

import pandas as pd
from pandas.errors import EmptyDataError

logger = logging.getLogger(__name__)


def main(concated_path: list, to_enrich_path: str, enriched_path: str | None = None):
    """
    concated_path: file where read concated streaming history
    to_enrich_path: file to write filtered streaming history
    enriched_path: file where already enriched streaming history
    """
    # Read track history Spotify file
    df_stream = pd.read_csv(concated_path)
    logger.info("%i rows to enrich", len(df_stream))

    try:
        df_already_enriched = pd.read_csv(enriched_path)
        logger.info("%i enriched rows found", len(df_already_enriched))
        # TODO there is an error : it lost new stream of a previously enriched track
        # df_stream = df_stream[(~df_stream.track_uri.isin(df_already_enriched.track_uri)) | (df_stream.end_time > max(df_already_enriched.end_time))] # noqa: ERA001, E501
        # df_stream = df_stream[(~df_stream.id.isin(df_already_enriched.id))]  noqa: ERA001
        df_stream = df_stream[df_stream.end_time > max(df_already_enriched.end_time)]
        logger.info("only %i rows to enrich", len(df_stream))
    except EmptyDataError:
        logger.warning("empty backup file found (%s)", enriched_path)
    except FileNotFoundError:
        logger.warning("no backup file found (%s)", enriched_path)

    # Drop NaN row
    df_stream = df_stream[df_stream["track_uri"].notna()]
    logger.info("%i rows to enrich without empty track_uri", len(df_stream))

    # Save stream to enrich
    df_stream.to_csv(to_enrich_path, mode="w", index=False)
    logger.info("%i rows are saved at %s", len(df_stream), to_enrich_path)
