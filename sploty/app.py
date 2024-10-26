from __future__ import annotations

from pathlib import Path

from pydantic import Field, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

from sploty import audio_features, concat, enrich, filter, metrics, to_elastic
from sploty.settings import logger


class Arguments(BaseSettings, cli_implicit_flags=True, cli_enforce_required=True):
    model_config = SettingsConfigDict(cli_parse_args=True)
    resources_path: str = Field(alias="resources-path", description="a required string")
    previous_enriched_streaming_history_path: str | None = Field(
        alias="previous-enriched-streaming-history-path",
        default=None,
        description="an optional string",
    )
    chunk_size: int = Field(alias="chunk-size", default=100, description="an optional int")
    spotify_timeout: int = Field(alias="spotify-timeout", default=10, description="an optional int")
    spotify_sleep: int = Field(alias="spotify-sleep", default=60, description="an optional int")
    db_path: str = Field(alias="db-path", description="a required string")
    index_name: str = Field(alias="index-name", description="a required string")
    elastic_timeout: int = Field(alias="elastic-timeout", default=10, description="an optional int")
    concat: bool = Field(alias="concat", default=True)
    filter: bool = Field(alias="filter", default=True)
    enrich: bool = Field(alias="enrich", default=True)
    feature: bool = Field(alias="feature", default=True)
    metric: bool = Field(alias="metric", default=True)
    elastic: bool = Field(alias="elastic", default=True)


class Environment(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    spotify_client_id: str = Field(description="a required string")
    spotify_client_secret: str = Field(description="a required string")
    spotify_auth_url: HttpUrl = Field(description="a required string")
    spotify_base_url: HttpUrl = Field(description="a required string")
    elastic_hosts: list[HttpUrl] = Field(description="a required string")
    elastic_user: str = Field(description="a required string")
    elastic_pass: str = Field(description="a required string")


def main() -> None:
    # Parse args and environment vars
    args = Arguments()
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
