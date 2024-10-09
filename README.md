# Sploty

Visualize and understand my Spotify data.

🚧 Work-in-progress repository

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

Retrieve the customer's id and secret and complete the `.env` file

```bash
SPOTIFY_CLIENT_ID="YOUR SPOTIFY CLIENT ID"
SPOTIFY_CLIENT_SECRET="YOUR SPOTIFY CLIENT SECRET"
SPOTIFY_AUTH_URL="https://accounts.spotify.com/api/token"
SPOTIFY_BASE_URL="https://api.spotify.com/v1/"
```

#### Elasticsearch

The final part (`to_elastic.py`) required Elasticsearch, have a look at [`docker-elk`](https://github.com/deviantony/docker-elk)

Retrieve host, username and password and complete the `.env` file

```bash
ELASTIC_HOSTS=["YOUR ELASTIC HOST"]
ELASTIC_USER="YOUR ELASTIC USERNAME"
ELASTIC_PASS="YOUR ELASTIC PASSWORD"
```

## How do I use Sploty?

### Download your data

1. Request your spotify data on [your spotify account](https://www.spotify.com/account/privacy/)
   - Select *Extended streaming history*"
   - Click on "*Request data*"
2. 30 days later
3. Open the mail from Spotify and download files

### Transform your data 

Run the app

```shell
poetry run python sploty/app.py \
  --resources-path your/path/to/the/extended_streaming_history_folder/ \
  --db-path your/path/to/a/folder/to/save/tracks/data \
  --index-name your-index-name
```

### Visualize your data

Open Kibana ([`http://localhost:5601`](http://localhost:5601) avec `docker-elk`) and create a dashboard to query your index

🚧 This part is not yet in the repository

![Image of a sample of Kibana board](img/kibana_board.png)
