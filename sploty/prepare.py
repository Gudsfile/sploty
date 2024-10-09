import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

CONFIG_FILE = "config.json"
with Path(CONFIG_FILE).open(encoding="utf8") as file:
    CONFIG = json.load(file)

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]

YOUR_LIBRARY_FILE = "YourLibrary.json"
YOUR_LIBRARY_PATH = RESOURCES_FOLDER + "/" + YOUR_LIBRARY_FILE

YOUR_LIBRARY_TRACKS_FILE = "YourLibrary_tracks.json"
YOUR_LIBRARY_TRACKS_PATH = RESOURCES_FOLDER + "/" + YOUR_LIBRARY_TRACKS_FILE

# Read library Spotify file
with Path(YOUR_LIBRARY_PATH).open(encoding="utf8") as file:
    your_library = json.load(file)

# Extract tracks
your_library_tracks = your_library["tracks"]
with Path(YOUR_LIBRARY_TRACKS_PATH).open("w", encoding="UTF-8") as file:
    json.dump(your_library_tracks, file)
logger.info("library tracks saved in %s", YOUR_LIBRARY_TRACKS_PATH)
