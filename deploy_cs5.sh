#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# CS5 local deployment helper (run on LOCAL Mac)
# Builds amd64 images (required from Apple Silicon) and pushes to Artifact Reg.
# -----------------------------------------------------------------------------

PROJECT_ID="project-22a0796a-596d-4fd9-b35"
REGION="us-central1"
REPO_PATH="us-central1-docker.pkg.dev/${PROJECT_ID}/pe-org-air-repo"

API_LOCAL_TAG="cs5-api:latest"
STREAMLIT_LOCAL_TAG="cs5-streamlit:latest"

API_REMOTE_TAG="${REPO_PATH}/cs5-api:latest"
STREAMLIT_REMOTE_TAG="${REPO_PATH}/cs5-streamlit:latest"

echo "==> Building FastAPI image (linux/amd64)"
# Build API image from a temporary Dockerfile that uses requirements.txt
# (avoids Poetry lockfile mismatch failures during CI/CD builds).
docker build --platform=linux/amd64 -t "${API_LOCAL_TAG}" -f - . <<'EOF'
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY src /app/src
COPY scripts /app/scripts
COPY pe_mcp /app/pe_mcp
COPY results /app/results

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
EOF

echo "==> Building Streamlit image (linux/amd64) using Dockerfile.streamlit"
docker build --platform=linux/amd64 -f Dockerfile.streamlit -t "${STREAMLIT_LOCAL_TAG}" .

echo "==> Tagging images for Artifact Registry"
docker tag "${API_LOCAL_TAG}" "${API_REMOTE_TAG}"
docker tag "${STREAMLIT_LOCAL_TAG}" "${STREAMLIT_REMOTE_TAG}"

echo "==> Pushing API image: ${API_REMOTE_TAG}"
docker push "${API_REMOTE_TAG}"

echo "==> Pushing Streamlit image: ${STREAMLIT_REMOTE_TAG}"
docker push "${STREAMLIT_REMOTE_TAG}"

echo "✅ CS5 images pushed successfully."
