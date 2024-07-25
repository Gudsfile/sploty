import json

from settings import logger

CONFIG_FILE = "config.json"
with open(CONFIG_FILE, encoding="utf8") as file:
    CONFIG = json.load(file)

# Files
RESOURCES_FOLDER = CONFIG["file"]["resources_folder"]

YOUR_LIBRARY_FILE = "YourLibrary.json"
YOUR_LIBRARY_PATH = RESOURCES_FOLDER + "/" + YOUR_LIBRARY_FILE

YOUR_LIBRARY_TRACKS_FILE = "YourLibrary_tracks.json"
YOUR_LIBRARY_TRACKS_PATH = RESOURCES_FOLDER + "/" + YOUR_LIBRARY_TRACKS_FILE

# Read library Spotify file
with open(YOUR_LIBRARY_PATH, encoding="utf8") as file:
    your_library = json.load(file)

# Extract tracks
your_library_tracks = your_library["tracks"]
with open(YOUR_LIBRARY_TRACKS_PATH, "w", encoding="UTF-8") as file:
    json.dump(your_library_tracks, file)
logger.info("library tracks saved in %s", YOUR_LIBRARY_TRACKS_PATH)
