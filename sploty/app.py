from pathlib import Path

import pydantic_argparse
from pydantic.v1 import BaseModel, Field

from sploty import concat, filter
from sploty.settings import logger


class Arguments(BaseModel):
    resources_path: str = Field(description="a required string")
    previous_enriched_streaming_history_path: str = Field(None, description="an optional string")


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
    # Process
    logger.info("============== CONCAT ==============")
    concat.main(streaming_history_paths, concated_streaming_history_path)
    logger.info("============== FILTER ==============")
    filter.main(
        concated_streaming_history_path,
        to_enrich_streaming_history_path,
        args.previous_enriched_streaming_history_path or enriched_streaming_history_path,
    )


if __name__ == "__main__":
    main()
