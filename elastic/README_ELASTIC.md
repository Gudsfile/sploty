# ELK

## Ingest pipeline

### `spotify-stream-pipeline`

```json
PUT _ingest/pipeline/spotify-stream-pipeline
{
  "processors": [
  {"rename": { "field": "artist_name", "target_field": "artist.name", "ignore_missing": true }},
  {"rename": { "field": "artist_uri", "target_field": "artist.uri", "ignore_missing": true }},
  {"rename": { "field": "artist_genres", "target_field": "artist.genres", "ignore_missing": true }},
  {"rename": { "field": "artist_popularity", "target_field": "artist.popularity", "ignore_missing": true }},
  {"rename": { "field": "track_name", "target_field": "track.name", "ignore_missing": true }},
  {"rename": { "field": "track_uri", "target_field": "track.uri", "ignore_missing": true }},
  {"rename": { "field": "track_duration_ms", "target_field": "track.duration_ms", "ignore_missing": true }},
  {"rename": { "field": "track_popularity", "target_field": "track.popularity", "ignore_missing": true }},
  {"rename": { "field": "track_is_in_library", "target_field": "track.is_in_library", "ignore_missing": true }},
  {"rename": { "field": "track_is_explicit", "target_field": "track.is_explicit", "ignore_missing": true }},
  {"rename": { "field": "track_is_local", "target_field": "track.is_local", "ignore_missing": true }},
  {"rename": { "field": "track_is_playable", "target_field": "track.is_playable", "ignore_missing": true }},
  {"rename": { "field": "album_uri", "target_field": "album.uri", "ignore_missing": true }},
  {"rename": { "field": "album_name", "target_field": "album.name", "ignore_missing": true }},
  {"rename": { "field": "album_type", "target_field": "album.type", "ignore_missing": true }},
  {"rename": { "field": "album_release_date", "target_field": "album.release_date", "ignore_missing": true }},
  {"rename": { "field": "stream_username", "target_field": "stream_context.username", "ignore_missing": true }},
  {"rename": { "field": "stream_platform", "target_field": "stream_context.platform", "ignore_missing": true }},
  {"rename": { "field": "stream_normalized_platform", "target_field": "stream_context.normalized_platform", "ignore_missing": true }},
  {"rename": { "field": "stream_conn_country", "target_field": "stream_context.conn_country", "ignore_missing": true }},
  {"rename": { "field": "stream_ip_addr_decrypted", "target_field": "stream_context.ip_addr_decrypted", "ignore_missing": true }},
  {"rename": { "field": "stream_user_agent_decrypted", "target_field": "stream_context.user_agent_decrypted", "ignore_missing": true }},
  {"rename": { "field": "stream_reason_start", "target_field": "stream_context.reason_start", "ignore_missing": true }},
  {"rename": { "field": "stream_reason_end", "target_field": "stream_context.reason_end", "ignore_missing": true }},
  {"rename": { "field": "stream_shuffle", "target_field": "stream_context.shuffle", "ignore_missing": true }},
  {"rename": { "field": "stream_skipped", "target_field": "stream_context.skipped", "ignore_missing": true }},
  {"rename": { "field": "stream_offline", "target_field": "stream_context.offline", "ignore_missing": true }},
  {"rename": { "field": "stream_offline_timestamp", "target_field": "stream_context.offline_timestamp", "ignore_missing": true }},
  {"rename": { "field": "stream_incognito_mode", "target_field": "stream_context.incognito_mode", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_danceability", "target_field": "audio_features.danceability", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_energy", "target_field": "audio_features.energy", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_key", "target_field": "audio_features.key", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_loudness", "target_field": "audio_features.loudness", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_mode", "target_field": "audio_features.mode", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_speechiness", "target_field": "audio_features.speechiness", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_acousticness", "target_field": "audio_features.acousticness", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_instrumentalness", "target_field": "audio_features.instrumentalness", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_liveness", "target_field": "audio_features.liveness", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_valence", "target_field": "audio_features.valence", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_tempo", "target_field": "audio_features.tempo", "ignore_missing": true }},
  {"rename": { "field": "track_audio_feature_time_signature", "target_field": "audio_features.time_signature", "ignore_missing": true }},
  {"user_agent": { "field": "stream_context.user_agent_decrypted", "ignore_missing": true }},
  {"convert": { "field": "stream_context.offline", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "stream_context.shuffle", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "stream_context.incognito_mode", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "stream_context.skipped", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "track.is_in_library", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "track.is_explicit", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "track.is_local", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "track.is_playable", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "is_new_track", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "is_new_artist", "type": "boolean", "ignore_missing": true }},
  {"convert": { "field": "is_new_album", "type": "boolean", "ignore_missing": true }}
  ]
}
```

## Component template

### `spotify-stream-mapping`

```json
PUT _component_template/spotify-stream-mapping
{
  "template": {
    "mappings": {
      "properties": {
        "audio_features": {
          "type": "object",
          "properties": {
            "mode": {"type": "integer"},
            "acousticness": {"type": "integer"},
            "loudness": {"type": "integer"},
            "liveness": {"type": "integer"},
            "tempo": {"type": "integer"},
            "valence": {"type": "integer"},
            "instrumentalness": {"type": "integer"},
            "danceability": {"type": "integer"},
            "key": {"type": "integer"},
            "speechiness": {"type": "integer"},
            "energy": {"type": "integer"},
            "time_signature": {"type": "integer"}
          }
        },
        "stream_context": {
          "properties": {
            "offline_timestamp": {"type": "date"},
            "user_agent_decrypted": {"type": "text"},
            "offline": {"type": "boolean"},
            "reason_end": {"type": "keyword"},
            "ip_addr_decrypted": {"type": "ip"},
            "reason_start": {"type": "keyword"},
            "conn_country": {"type": "keyword"},
            "shuffle": {"type": "boolean"},
            "incognito_mode": {"type": "boolean"},
            "platform": {"type": "text"},
            "normalized_platform": {"type": "keyword"},
            "username": {"type": "keyword"},
            "skipped": {"type": "boolean"}
          }
        },
        "artist": {
          "properties": {
            "genres": {"type": "text"},
            "popularity": {"type": "long"},
            "name": {"type": "keyword"},
            "uri": {"type": "keyword" }
          }
        },
        "album": {
          "properties": {
            "name": {"type": "keyword"},
            "uri": {"type": "keyword"},
            "type": {"type": "keyword"},
            "release_date": {"format": "year||year_month||year_month_day", "type": "date"}
          }
        },
        "track": {
          "properties": {
            "duration_ms": {"type": "long"},
            "popularity": {"type": "long"},
            "name": {"type": "keyword"},
            "uri": {"type": "keyword"},
            "is_in_library": {"type": "boolean"},
            "is_explicit": {"type": "boolean"},
            "is_local": {"type": "boolean"},
            "is_playable": {"type": "boolean"}
          }
        },
        "end_time": {"format": "yyyy-MM-dd'T'HH:mm:ss'Z'", "type": "date"},
        "percentage_played": {"type": "long"},
        "day_name": {"type": "keyword"},
        "month_name": {"type": "keyword"},
        "ms_played": {"type": "long"},
        "min_played": {"type": "long"},
        "is_new_track": {"type": "boolean"},
        "is_new_artist": {"type": "boolean"},
        "is_new_album": {"type": "boolean"}
      }
    }
  }
}
```

### `spotify-stream-setting`

```json
PUT _component_template/spotify-stream-setting
{
  "template": {
    "settings": {
    "index": {
      "number_of_shards": "1",
      "number_of_replicas": "0",
      "default_pipeline": "spotify-stream-pipeline"
    }
  }
  }
}
```

## Index template 

### `spotify-stream-template`

```json
PUT _index_template/spotify-stream-template
{
  "index_patterns": [
    "spotify-stream-*"
  ],
  "composed_of": [
    "spotify-stream-mapping",
    "spotify-stream-setting"
  ]
}
```

## Index 

(créés par le script python)

### `spotify-stream-history-v1`

## Data view

### `Spotify Understand * data`

```
Index pattern: spotify-stream-*
Time field: end_time
```
