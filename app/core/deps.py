# app/core/deps.py
import os
from functools import lru_cache

from app.services.redis_cache import RedisCache
from app.config import settings


@lru_cache
def get_cache() -> RedisCache:
    # settings.REDIS_URL comes from .env (local) or docker-compose env (container)
    return RedisCache(settings.REDIS_URL)


# simple global for routers to import
cache = get_cache()


# ---------------------------------------------------------------------------
# Shared HybridRetriever — single instance across all routers
# ---------------------------------------------------------------------------

from src.services.retrieval.hybrid import HybridRetriever  # noqa: E402


@lru_cache
def get_retriever() -> HybridRetriever:
    persist_dir = getattr(settings, "CHROMA_PERSIST_DIR", None) or os.getenv("CHROMA_PERSIST_DIR", "./chroma_data")
    return HybridRetriever(persist_dir=persist_dir)


# simple global for routers to import (same pattern as cache above)
retriever = get_retriever()