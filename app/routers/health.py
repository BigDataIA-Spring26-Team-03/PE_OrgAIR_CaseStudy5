from datetime import datetime, timezone
from typing import Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.config import settings
from app.core.deps import cache
from app.services.snowflake import db

router = APIRouter(tags=["Health"])


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    dependencies: Dict[str, str]


@router.get("/health", response_model=HealthResponse)
async def health_check():
    dependencies: Dict[str, str] = {}

    # Snowflake
    dependencies["snowflake"] = await db.check_health()

    # Redis
    try:
        cache.client.ping()
        dependencies["redis"] = "healthy"
    except Exception:
        dependencies["redis"] = "unhealthy"

    # S3 (placeholder: not configured unless bucket + keys exist)
    if settings.S3_BUCKET_NAME and settings.AWS_ACCESS_KEY_ID and settings.AWS_SECRET_ACCESS_KEY:
        dependencies["s3"] = "configured"
    else:
        dependencies["s3"] = "not_configured"

    all_healthy = all(
        dependencies[k] == "healthy" for k in ["snowflake", "redis"]
    )

    if not all_healthy:
        # case-study friendly: return 503 when core deps are down
        raise HTTPException(
            status_code=503,
            detail=HealthResponse(
                status="degraded",
                timestamp=datetime.now(timezone.utc),
                version=settings.APP_VERSION,
                dependencies=dependencies,
            ).model_dump(mode="json"),
        )

    return HealthResponse(
        status="healthy",
        timestamp=datetime.now(timezone.utc),
        version=settings.APP_VERSION,
        dependencies=dependencies,
    )