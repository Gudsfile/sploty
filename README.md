# Sploty

Visualize and understand my Spotify data.

- [How do I configure Sploty?](#how-do-i-configure-sploty)
- [How do I use Sploty?](#how-do-i-use-sploty)

## How do I configure Sploty? 

Clone the repository

Install [Poetry](https://python-poetry.org)

Create the virtualenv with Poetry

```shell
poetry install
poetry run python --version
```

The config file must be copied and completed

```shell
cp config.default.json config.json
```

- [Complete it with Spotify](#spotify)
- [Complete it with Elasticsearch](#elasticsearch)

#### Spotify

Sploty requires a Spotify developer account, look at the [Spotify documentation](https://developer.spotify.com/documentation/web-api/tutorials/getting-started) to set it up

Retrieve the customer's id and secret and complete the `config.json` file

```json
    "spotify": {
        …
        "client_id": "TO BE COMPLETED",
        "client_secret": "TO BE COMPLETED",
        …
    },
```

#### Elasticsearch

The final part (`to_elastic.py`) required Elasticsearch, have a look at [`docker-elk`](https://github.com/deviantony/docker-elk)

Retrieve host, useername and password and complete the `config.json` file

```json
    "elasticsearch": {
        "hosts": [
            "TO BE COMPLETED"
        ],
        "username": "TO BE COMPLETED",
        "password": "TO BE COMPLETED",
        …
    },
```

## How do I use Sploty?

### Download your data

1. Request your spotify data on [your spotify account](https://www.spotify.com/account/privacy/)
   - Select *Extended streaming history*"
   - Click on "*Request data*"
2. 30 days later
3. Open the mail from Spotify and download files

### Transform your data 

1. Concat all streams files with
   ```shell
   poetry run python concat.py
   ```
2. Filter already enriched streams with
   ```shell
   poetry run python filter.py
   ```
3. Enrich spotify metadata with
   ```shell
   poetry run python enrich.py
   ```
4. Enrich spotify audio features with 
   ```shell
   poetry run python audio_features.py
   ```
   :bug: There's a bug: there's a shift in the columns
5. Index their to elastic with
   ```shell
   poetry run python to_elastic.py
   ```
   This part required Elasticsearch, have a look at [`docker-elk`](https://github.com/deviantony/docker-elk)

### Visualize your data

Open Kibana ([`http://localhost:5601`](http://localhost:5601) avec `docker-elk`) and create a dashboard to query your index
