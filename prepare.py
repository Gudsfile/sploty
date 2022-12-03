import json

CONFIG_FILE = 'config.json'
CONFIG = json.load(open(CONFIG_FILE, 'r', encoding='UTF-8'))

RESOURCES_FOLDER = CONFIG['file']['resources_folder']

YOUR_LIBRARY_FILE = 'YourLibrary.json'
YOUR_LIBRARY_PATH = RESOURCES_FOLDER + '/' + YOUR_LIBRARY_FILE

YOUR_LIBRARY_TRACKS_FILE = 'YourLibrary_tracks.json'
YOUR_LIBRARY_TRACKS_PATH = RESOURCES_FOLDER + '/' + YOUR_LIBRARY_TRACKS_FILE

your_library = json.load(open(YOUR_LIBRARY_PATH, 'r', encoding='UTF-8'))
your_library_tracks = your_library['tracks']
json.dump(your_library_tracks, open(YOUR_LIBRARY_TRACKS_PATH, 'w', encoding='UTF-8'))
print(f'INFO - library tracks saved in {YOUR_LIBRARY_TRACKS_PATH}')
