#!/bin/bash
set -euo pipefail

PROJECT="project-22a0796a-596d-4fd9-b35"
REGION="us-central1"
REPO="pe-org-air-repo"
VM="pe-org-air-vm"
ZONE="us-central1-a"

API_IMAGE_LOCAL="pe-org-air-cs4-api"
ST_IMAGE_LOCAL="pe-org-air-cs4-streamlit"

API_IMAGE_REMOTE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/cs4-api:latest"
ST_IMAGE_REMOTE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/cs4-streamlit:latest"

echo "=== Building CS4 API image (linux/amd64) ==="
docker build --platform=linux/amd64 -t "${API_IMAGE_LOCAL}" .
docker tag "${API_IMAGE_LOCAL}" "${API_IMAGE_REMOTE}"
docker push "${API_IMAGE_REMOTE}"

echo "=== Building CS4 Streamlit image (linux/amd64) ==="
docker build --platform=linux/amd64 -f Dockerfile.streamlit -t "${ST_IMAGE_LOCAL}" .
docker tag "${ST_IMAGE_LOCAL}" "${ST_IMAGE_REMOTE}"
docker push "${ST_IMAGE_REMOTE}"

echo "=== Images pushed. Now SSH into VM and run Phase 2 ==="
echo "Run: gcloud compute ssh ${VM} --zone=${ZONE}"

cat <<'EOF'

========================
PHASE 2 (run on the VM)
========================

# Authenticate Docker to Artifact Registry
docker-credential-gcr configure-docker --registries=us-central1-docker.pkg.dev

# Create chroma_data directory on VM for persistence
mkdir -p /home/ishaansamel/chroma_data

# Stop old containers
docker stop api streamlit 2>/dev/null || true
docker rm api streamlit 2>/dev/null || true

# Pull latest images
docker pull us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-api:latest
docker pull us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-streamlit:latest

# Run CS4 API (fill in secrets)
docker run -d --name api --restart=always \
  -p 8000:8000 \
  --network=pe-net \
  -v /home/ishaansamel/chroma_data:/app/chroma_data \
  -e SNOWFLAKE_ACCOUNT=AULIZOV-DXC76868 \
  -e SNOWFLAKE_USER=ISHAANSAMEL11 \
  -e SNOWFLAKE_PASSWORD=<PASSWORD> \
  -e SNOWFLAKE_DATABASE=PE_ORGAIR_DB \
  -e SNOWFLAKE_SCHEMA=PE_ORGAIR_SCHEMA \
  -e SNOWFLAKE_WAREHOUSE=PE_ORGAIR_WH \
  -e REDIS_URL=redis://redis:6379/0 \
  -e AWS_ACCESS_KEY_ID=<KEY> \
  -e AWS_SECRET_ACCESS_KEY=<SECRET> \
  -e AWS_REGION=us-east-1 \
  -e S3_BUCKET=pe-org-air-group3-storage \
  -e S3_BUCKET_NAME=pe-org-air-group3-storage \
  -e OPENAI_API_KEY=<KEY> \
  -e CHROMA_PERSIST_DIR=/app/chroma_data \
  -e CS1_BASE_URL=http://localhost:8000 \
  -e CS2_BASE_URL=http://localhost:8000 \
  -e CS3_BASE_URL=http://localhost:8000 \
  us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-api:latest

# Run CS4 Streamlit
docker run -d --name streamlit --restart=always \
  -p 8501:8501 \
  --network=pe-net \
  -e API_BASE_URL=http://api:8000 \
  us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-streamlit:latest

echo "=== Verify health ==="
sleep 10
curl http://localhost:8000/health

EOF

