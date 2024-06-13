# Spotify Understand My Data

## Setup

Clone the repository

Install [Poetry](https://python-poetry.org)

Create the virtualenv with Poetry

```shell
poetry install
poetry run python --version
```

## Workflow

- Download spotify data
- Extract files
- ~~Extract "tracks" part of the file YourLibrary.json in a file called Yourlibrary_tracks.json with `prepare.py`~~
- Concat all streams files with `concat.py`
- Filter already enriched streams with `filter.py`
- Enrich spotify metadata with `enrich.py`
- Enrich spotify audio features with `audio_features.py` (bug : y'a un décalage dans les colonnes)
- Index their to elastic with `to_elastic.py`
