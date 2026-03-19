## CS4 Deployment (GCP VM + Artifact Registry)

This repository deploys **one FastAPI app** (CS1–CS4) plus a **Streamlit UI** as separate containers on a GCP VM.

### Prerequisites

- **Docker** installed locally (Mac).
- **gcloud CLI** installed locally and authenticated to your project.
- **Apple Silicon note**: ALWAYS build images with `--platform=linux/amd64`.
- Artifact Registry exists:
  - `us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/`

### Environment variables (do not commit)

The API container needs (examples):

- **Snowflake**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE`
- **Redis**: `REDIS_URL=redis://redis:6379/0`
- **S3**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET`, `S3_BUCKET_NAME`
- **LLM**: `OPENAI_API_KEY`
- **Chroma**: `CHROMA_PERSIST_DIR=/app/chroma_data`
- **CS URLs**: `CS1_BASE_URL`, `CS2_BASE_URL`, `CS3_BASE_URL` (all `http://localhost:8000`)

The Streamlit container needs:

- `API_BASE_URL=http://api:8000`

### Phase 1 (Local Mac): Build + Push

From repo root:

```bash
chmod +x deploy_cs4.sh
./deploy_cs4.sh
```

### Phase 2 (On VM): Pull + Run

SSH into the VM:

```bash
gcloud compute ssh pe-org-air-vm --zone=us-central1-a
```

On the VM:

```bash
# Authenticate Docker to Artifact Registry (COS-friendly)
docker-credential-gcr configure-docker --registries=us-central1-docker.pkg.dev

# Persist ChromaDB across restarts
mkdir -p /home/ishaansamel/chroma_data

# Stop old containers (ignore if missing)
docker stop api streamlit 2>/dev/null || true
docker rm api streamlit 2>/dev/null || true

# Pull latest
docker pull us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-api:latest
docker pull us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-streamlit:latest

# Run API (fill in secrets)
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

# Run Streamlit
docker run -d --name streamlit --restart=always \
  -p 8501:8501 \
  --network=pe-net \
  -e API_BASE_URL=http://api:8000 \
  us-central1-docker.pkg.dev/project-22a0796a-596d-4fd9-b35/pe-org-air-repo/cs4-streamlit:latest

# Verify
sleep 10
curl http://localhost:8000/health
```

### ChromaDB auto-indexing

On first boot, if ChromaDB is empty, the API will automatically fetch evidence from Snowflake (via the same logic as `/api/v1/evidence`) and index it into ChromaDB.

### Live URLs

- FastAPI health: `http://34.60.223.69:8000/health`
- FastAPI docs: `http://34.60.223.69:8000/docs`
- Streamlit: `http://34.60.223.69:8501`

### Troubleshooting

- **ARM/AMD64 mismatch**: Always build with `--platform=linux/amd64`.
- **Streamlit can’t reach API**: ensure Streamlit uses `API_BASE_URL=http://api:8000` (container name).
- **Snowflake token expiry**: restart API container: `docker restart api`.
- **Chroma empty**: check API logs: `docker logs -f api` for `chroma_empty_starting_index`.
- **Do not deploy Airflow**: use `docker-compose.gcp.yml` or the two `docker run` commands above.

