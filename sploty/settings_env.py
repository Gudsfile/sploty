from __future__ import annotations

import logging
from typing import Dict

from pydantic import BaseModel, DirectoryPath, FilePath, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggerConf(BaseModel):
    level: str = logging.INFO

class InputConf(BaseModel):
    resource_folder: DirectoryPath
    chunk_size: int

class SpotifyConf(BaseModel):
        auth_url: HttpUrl = "https://accounts.spotify.com/api/token"
        base_url: HttpUrl = "https://api.spotify.com/v1/"
        client_id: str
        client_secret: str
        timeout: int= 10
        s_sleep: int = 60

class ElasticsearchIndexConf(BaseModel):
    name: str

class ElasticsearchConf(BaseModel):
    host: list[HttpUrl] = ["http://localhost:9200"]
    username: str = "elastic"
    password: str = "changeme"
    request_timeout: int = 10
    index : ElasticsearchIndexConf

class TinyDBConf(BaseModel):
    tables: Dict[str, FilePath]

class Settings(BaseSettings):
    logger_conf: LoggerConf = LoggerConf()
    input_conf: InputConf = InputConf()
    spotify_conf: SpotifyConf = SpotifyConf()
    elasticsearch_index_conf: ElasticsearchIndexConf = ElasticsearchIndexConf()
    elasticsearch_conf: ElasticsearchConf = ElasticsearchConf()
    tinydb_conf: TinyDBConf = TinyDBConf()

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings.model_validate({})
