import os
from pathlib import Path

import pydantic_argparse
from pydantic.v1 import BaseModel, Field

from sploty import concat, enrich, filter
from sploty.settings import logger


class Arguments(BaseModel):
    resources_path: str = Field(description="a required string")
    previous_enriched_streaming_history_path: str = Field(None, description="an optional string")
    chunk_size: int = Field(default=100, description="an optional int")
    spotify_timeout: int = Field(default=10, description="an optional int")
    spotify_sleep: int = Field(default=60, description="an optional int")
    concat: bool = Field(default=True)
    filter: bool = Field(default=True)
    enrich: bool = Field(default=True)
    feature: bool = Field(default=True)
    elastic: bool = Field(default=True)


def main() -> None:
    # Create Parser and Parse Args
    parser = pydantic_argparse.ArgumentParser(
        model=Arguments,
        prog="Example Program",
        description="Example Description",
        version="0.0.1",
        epilog="Example Epilog",
    )
    args = parser.parse_typed_args()

    # Paths
    resources_path = args.resources_path
    streaming_history_paths = list(Path(resources_path).glob("Streaming_History_Audio_*.json"))

    concated_streaming_history_path = Path(f"{resources_path}/sploty_concated_history.csv")
    to_enrich_streaming_history_path = Path(f"{resources_path}/sploty_filtered_history.csv")
    enriched_streaming_history_path = Path(f"{resources_path}/sploty_enriched_history.csv")
    featured_streaming_history_path = Path(f"{resources_path}/sploty_featured_history.csv")  # noqa: F841

    # Process
    logger.info("============== CONCAT ==============")
    if args.concat:
        concat.main(streaming_history_paths, concated_streaming_history_path)
    else:
        logger.info("skip")
    logger.info("============== FILTER ==============")
    if args.filter:
        filter.main(
            concated_streaming_history_path,
            to_enrich_streaming_history_path,
            args.previous_enriched_streaming_history_path or enriched_streaming_history_path,
        )
    else:
        logger.info("skip")
    logger.info("============== ENRICH ==============")
    if args.enrich:
        spotify_api_params = enrich.SpotifyApiParams(
            base_url="https://api.spotify.com/v1/",
            endpoint="tracks",
            url="https://api.spotify.com/v1/tracks/",
            headers=enrich.get_spotify_auth_header(
                "https://accounts.spotify.com/api/token",
                os.environ["SPOTIFY_CLIENT_ID"],
                os.environ["SPOTIFY_CLIENT_SECRET"],
                args.spotify_timeout,
            ),
            timeout=args.spotify_timeout,
            sleep=args.spotify_sleep,
        )
        enrich.main(
            to_enrich_streaming_history_path,
            enriched_streaming_history_path,
            args.chunk_size,
            spotify_api_params,
        )
    else:
        logger.info("skip")


if __name__ == "__main__":
    main()
