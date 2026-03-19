from app.config import settings
print(f"RapidAPI Key (first 20 chars): {settings.rapidapi_key[:20] if hasattr(settings, 'rapidapi_key') else 'NOT FOUND'}")
