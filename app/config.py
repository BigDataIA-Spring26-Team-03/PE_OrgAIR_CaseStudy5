from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache
from typing import Optional
from pydantic import model_validator
from pydantic_settings import SettingsConfigDict
from pydantic import Field
from pydantic import AliasChoices


class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables.
    """
    # Application
    APP_ENV: str = "development"
    DEBUG: bool = True
    APP_NAME: str = "PE OrgAIR Platform"
    APP_VERSION: str = "1.0.0"
    
    # Snowflake (Optional for testing, required for production)
    SNOWFLAKE_ACCOUNT: Optional[str] = None
    SNOWFLAKE_USER: Optional[str] = None
    SNOWFLAKE_PASSWORD: Optional[str] = None
    SNOWFLAKE_DATABASE: str = "PE_ORGAIR_DB"
    SNOWFLAKE_SCHEMA: str = "PE_ORGAIR_SCHEMA"
    SNOWFLAKE_WAREHOUSE: str = "PE_ORGAIR_WH"
    
    # Redis
    REDIS_URL: str = Field(default="redis://localhost:6379/0")
    
    # AWS S3
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
    AWS_REGION: Optional[str] = "us-east-1"
    S3_BUCKET_NAME: Optional[str] = Field(default=None, validation_alias=AliasChoices("S3_BUCKET", "S3_BUCKET_NAME"))


    SEC_EDGAR_USER_AGENT_EMAIL: str
    SEC_SLEEP_SECONDS: float = 0.75
    RAPIDAPI_KEY: str = Field(default="")
    OPENAI_API_KEY: str = Field(default="")

    # CS4: LiteLLM multi-provider routing
    ANTHROPIC_API_KEY: str = Field(default="")
    LITELLM_BUDGET_USD_PER_DAY: float = Field(default=50.0)

    
    # NEW: USPTO API (for CS2)
    uspto_api_key: Optional[str] = None
    
    # API Settings
    api_version: str = "1.0.0"
    
    @model_validator(mode="after")
    def require_snowflake_in_production(self):
        if self.APP_ENV == "production":
            missing = [
                k for k in [
                    "SNOWFLAKE_ACCOUNT",
                    "SNOWFLAKE_USER",
                    "SNOWFLAKE_PASSWORD",
                ]
                if getattr(self, k) in (None, "")
            ]
            if missing:
                raise ValueError(
                    f"Missing Snowflake settings in production: {missing}"
                )
        return self
    
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )

@lru_cache
def get_settings() -> Settings:
    """
    Cached settings instance to avoid repeated env loading.
    """
    return Settings()

settings = get_settings()