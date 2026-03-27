#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# CS5 VM deployment helper (run INSIDE the GCP VM)
# Pull latest images and run FastAPI + Streamlit containers on pe-net network.
# -----------------------------------------------------------------------------

PROJECT_ID="project-22a0796a-596d-4fd9-b35"
REPO_PATH="us-central1-docker.pkg.dev/${PROJECT_ID}/pe-org-air-repo"
API_IMAGE="${REPO_PATH}/cs5-api:latest"
STREAMLIT_IMAGE="${REPO_PATH}/cs5-streamlit:latest"

echo "==> Stopping/removing old containers if present"
docker rm -f api streamlit 2>/dev/null || true

echo "==> Pulling latest CS5 images"
docker pull "${API_IMAGE}"
docker pull "${STREAMLIT_IMAGE}"

echo "==> Starting FastAPI container (api)"
docker run -d \
  --name api \
  --network pe-net \
  -p 8000:8000 \
  --restart always \
  -v /home/ishaansamel/chroma_data:/app/chroma_data \
  -e SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-}" \
  -e SNOWFLAKE_USER="${SNOWFLAKE_USER:-}" \
  -e SNOWFLAKE_PASSWORD="${SNOWFLAKE_PASSWORD:-}" \
  -e SNOWFLAKE_DATABASE="PE_ORGAIR_DB" \
  -e SNOWFLAKE_SCHEMA="PE_ORGAIR_SCHEMA" \
  -e SNOWFLAKE_WAREHOUSE="PE_ORGAIR_WH" \
  -e REDIS_URL="redis://redis:6379/0" \
  -e OPENAI_API_KEY="${OPENAI_API_KEY:-}" \
  -e ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" \
  -e CHROMA_PERSIST_DIR="/app/chroma_data" \
  "${API_IMAGE}"

echo "==> Starting Streamlit container (streamlit)"
docker run -d \
  --name streamlit \
  --network pe-net \
  -p 8501:8501 \
  --restart always \
  -e API_BASE_URL="http://api:8000" \
  "${STREAMLIT_IMAGE}"

echo "✅ VM setup complete."
echo "FastAPI  : http://34.60.223.69:8000"
echo "Dashboard: http://34.60.223.69:8501"
