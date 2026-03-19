# app/cache/redis_cache.py
from __future__ import annotations

from typing import Optional, Type, TypeVar, Any
import json

import redis
from pydantic import BaseModel
from redis.exceptions import RedisError

T = TypeVar("T", bound=BaseModel)


class RedisCache:
    def __init__(self, url: str):
        self.client = redis.from_url(url, decode_responses=True)

    def get(self, key: str, model: Type[T]) -> Optional[T]:
        try:
            data = self.client.get(key)
            if data:
                return model.model_validate_json(data)
        except RedisError:
            return None
        return None

    def set(self, key: str, value: BaseModel, ttl_seconds: int) -> None:
        try:
            self.client.setex(key, ttl_seconds, value.model_dump_json())
        except RedisError:
            return None

    def get_json(self, key: str) -> Optional[Any]:
        try:
            data = self.client.get(key)
            return json.loads(data) if data else None
        except (RedisError, json.JSONDecodeError):
            return None

    def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        try:
            self.client.setex(key, ttl_seconds, json.dumps(value))
        except RedisError:
            return None

    def delete(self, key: str) -> None:
        try:
            self.client.delete(key)
        except RedisError:
            return None

    def delete_pattern(self, pattern: str) -> None:
        try:
            for key in self.client.scan_iter(match=pattern):
                self.client.delete(key)
        except RedisError:
            return None