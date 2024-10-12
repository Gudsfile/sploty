from __future__ import annotations

from pathlib import Path

import pydantic_argparse
from pydantic import Field, HttpUrl, v1
from pydantic_settings import BaseSettings

from sploty import audio_features, concat, enrich, filter, metrics, to_elastic
from sploty.settings import logger


class Arguments(v1.BaseModel):
    resources_path: str = v1.Field(description="a required string")
    previous_enriched_streaming_history_path: str = v1.Field(None, description="an optional string")
    chunk_size: int = v1.Field(default=100, description="an optional int")
    spotify_timeout: int = v1.Field(default=10, description="an optional int")
    spotify_sleep: int = v1.Field(default=60, description="an optional int")
    db_path: str = v1.Field(description="a required string")
    index_name: str = v1.Field(description="a required string")
    elastic_timeout: int = v1.Field(default=10, description="an optional int")
    concat: bool = v1.Field(default=True)
    filter: bool = v1.Field(default=True)
    enrich: bool = v1.Field(default=True)
    feature: bool = v1.Field(default=True)
    metric: bool = v1.Field(default=True)
    elastic: bool = v1.Field(default=True)


class Environment(BaseSettings):
    spotify_client_id: str = Field(description="a required string")
    spotify_client_secret: str = Field(description="a required string")
    spotify_auth_url: HttpUrl = Field(description="a required string")
    spotify_base_url: HttpUrl = Field(description="a required string")
    elastic_hosts: list[HttpUrl] = Field(description="a required string")
    elastic_user: str = Field(description="a required string")
    elastic_pass: str = Field(description="a required string")


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
    env = Environment()

    # Paths
    resources_path = args.resources_path
    streaming_history_paths = list(Path(resources_path).glob("Streaming_History_Audio_*.json"))

    concated_streaming_history_path = Path(f"{resources_path}/sploty_concated_history.csv")
    to_enrich_streaming_history_path = Path(f"{resources_path}/sploty_filtered_history.csv")
    enriched_streaming_history_path = Path(f"{resources_path}/sploty_enriched_history.csv")
    featured_streaming_history_path = Path(f"{resources_path}/sploty_featured_history.csv")
    metrics_streaming_history_path = Path(f"{resources_path}/sploty_metrics_history.csv")

    db_path = args.db_path
    audio_features_db_path = Path(f"{db_path}/tracks.json")

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
                env.spotify_client_id,
                env.spotify_client_secret,
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
    logger.info("============= FEATURES =============")
    if args.feature:
        db = audio_features.get_db(audio_features_db_path)
        spotify_api_params = enrich.SpotifyApiParams(
            base_url="https://api.spotify.com/v1/",
            endpoint="audio-features",
            url="https://api.spotify.com/v1/audio-features/",
            headers=enrich.get_spotify_auth_header(
                "https://accounts.spotify.com/api/token",
                env.spotify_client_id,
                env.spotify_client_secret,
                args.spotify_timeout,
            ),
            timeout=args.spotify_timeout,
            sleep=args.spotify_sleep,
        )
        audio_features.main(
            to_enrich_streaming_history_path,
            enriched_streaming_history_path,
            featured_streaming_history_path,
            args.chunk_size,
            spotify_api_params,
            db,
        )
    else:
        logger.info("skip")
    logger.info("============== METRICS =============")
    if args.metric:
        metrics.main(featured_streaming_history_path, metrics_streaming_history_path)
    else:
        logger.info("skip")
    logger.info("============== ELASTIC =============")
    if args.elastic:
        elastic = to_elastic.get_elastic(
            list(map(str, env.elastic_hosts)),
            env.elastic_user,
            env.elastic_pass,
            args.elastic_timeout,
        )
        to_elastic.main(metrics_streaming_history_path, args.index_name, elastic)
    else:
        logger.info("skip")


if __name__ == "__main__":
    main()
