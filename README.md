# PE Org-AI-R Platform
## Case Study 4 — RAG & Search Engine
### From Scored Evidence to Cited Investment Justifications

**Course:** Big Data and Intelligent Analytics
**Instructor:** Professor Sri Krishnamurthy
**Term:** Spring 2026

## Team 3

- Ishaan Samel
- Ayush Fulsundar
- Vaishnavi Srinivas

---

## 🚀 Live Application

| Component | Link |
|-----------|------|
| Demo Video | [Demo Video](https://youtu.be/ZU32gMl1m3g) |
| Interactive Codelab | [CS4 RAG & Search Codelab](https://codelabs-preview.appspot.com/?file_id=1vbScSJyPROzPjuzx6h-lVBne2yIizObxuDUvmVDJmBI#12) 
| Deployed Fast API Link | http://34.60.223.69:8000/docs |
| Deployed App Link | http://34.60.223.69:8501|

---

## 📌 Executive Summary

Case Study 4 implements the RAG (Retrieval-Augmented Generation) & Search layer of the PE Org-AI-R platform.

Building on:

- Case Study 1 → Platform Foundation (FastAPI, Snowflake, Redis, Docker)
- Case Study 2 → Evidence Collection & Signal Extraction
- Case Study 3 → Risk-adjusted, sector-calibrated scoring engine
- Case Study 4 → RAG-powered search, score justification, and IC meeting preparation

The CS4 layer answers the critical PE question:

> *"Why did this company score 72 on Data Infrastructure?"*

It transforms raw Org-AI-R scores into **cited, evidence-backed investment justifications** suitable for IC (Investment Committee) presentations.

Key capabilities:

- Hybrid semantic + keyword evidence search (Dense + BM25 + RRF fusion)
- HyDE query enhancement for better retrieval
- Score justification generation with cited evidence
- IC Meeting Prep package generation across all 7 dimensions
- Analyst Notes Collector for post-LOI due diligence evidence
- On-demand company onboarding for any US-listed ticker
- Dynamic leadership signals via Wikidata + Wikipedia enrichment

---

## 🏗 System Architecture Overview

|  Architecture Diagram Link | [DrawIo](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1&dark=auto#R%3Cmxfile%3E%3Cdiagram%20name%3D%22Page-1%22%20id%3D%227lTkgS7lPNfq_iGpEu2w%22%3E1Vvbcts2EP0azbQPyvAu6VHRxXFrNx6rl7QvHoiERDgkwYKQZObrC%2FAuEIpkGopZZ%2ByQIEiB5yx2zy6ggTkLX24IiP177MFgYGjey8CcDwzD0Ayb%2Fcdb0rxlbGh5w5YgL2%2FS64YV%2BgaLxrLbDnkwOepIMQ4oio8bXRxF0KVHbYAQfDjutsHB8afGYAtbDSsXBO3Wv5BH%2FfItRnX7J4i2fvnJujPJr4Sg7Fy8SeIDDx8aTeZiYM4IxjQ%2FCl9mMODglbjk9y1PXK0GRmBEL7nBaN9QPCOhafm6FL6wax99GgasQWeHGxzRghZ9XJ4XN%2FDrIEDbiB277KmQsIb8mXsQ7IpnzlYWaxgsjMFYG0z48cOC%2FflMtsPp7fCRHa7ShMKQHUyJ6yPKeNwRWDwHEjamxnCLl7uBOISUpKyL38DfLMA%2B1FyNtKKtssniPC3PC2spLGVbPbkGkx0UeMqx9ZJLwE0OKAxABAVUTQmqGxQEMxxgkt1pbmz%2Bj7UnlOCvsLwS4ephjc75j4yJOaCAw413xGWTir8566TNVoZA0GLPZl3kQn4RBwEjBOGoEyG61mZE1yfHlAwnx5SMlFAimSElTw1SCN5FHvQK3A%2Fc%2FFYxcPnVA3NpEi6ynxYX7IqT%2FQjs6jIiVosZR3l%2BM2Xm77BbmbszP%2Bra8Ffuo1CAom3SCe6RBG1bsH9DsH9bBdhWf8G%2BCUCSeJjdWEPt7oLcx2gE7hE8dEPbOY%2B2JXgbU4lp2%2F1F%2Bw5FX6F3G2XeZcn%2B3rIxsEHU2D%2FjNR87TmhnM78AeHt8DeCd%2FgL%2FEQPi8eBK8EvagDv3NfMF50K3pp3wtu2zeI8cAW%2FjgwLH4iYXyZZ3j6zdYqNErehVY%2FkcwV1XsfONsII2rhXYbzFlz4Zjz5KZ8thYm5eZ8pqb8pObKw9MPsRNe%2B4E9ViCtCEAbap3GAzRdY9xjgFlg3pKmIIHQXIlmEeC%2FHCsa8Ds9hjmAAIPksRH8ZWhLt10qT1El6wGaq%2FHUG9LrXfKe9TCr6BCGfjjY%2FAd01YO%2FuHXxP8DkO2X5fqPgx%2BEu%2FAODsux9JKP36Hr84xTMdaC6x7q6g0dPDEQ2a%2BZPwp6rTpRW5BkefVpBvgzSimCCfXxFjNUFnVrgR9lFMOKSVMqNtqYERgAivbHo3zbVDdPv2qPtNdsZQ6OyxcrFxOWW%2FAUO9qiqFspSdclhqc5QjQTDE9F5YLhLhVn5tunNNTZpB7JpvTEGZngoikNi9rQUwjiGDaT6hHr6KEQRgnCkbrJPhKi2jWCmimVaT1BnOzWBLlPCbPqI7wDuM%2Bq7Xpp%2BzzkaC4I0JqAzhU7GQWi1V9FKptSDdcTDv58HJQFjU%2BPOcpMSgCmL5IGI0mmOIaVK2bjLl1RByZkxTxxMlwh8jHMpRKvJ0wkaQTJNh2U1WsXR5uyWl0RMWEw8ckwu2V%2Fflot7n%2FuxICsziTk545Y4FNS9TBhfxgQo3iLkeZKDvdRDR42iCkc1o53NN5RVRywTOeIg5EYExRwwPWfyX6LbPX1%2Bq9NVjf9Z7XwlmN2sf7Lbp0SAtJGhxijiCaNJz%2FwhhpiW8glrYmw2vi6%2FuwgH0HNRfUqF04RyYpDL8WpuPj5OL3JfJcDwji7Q1tBQHia1EWkTl4vUm01QcKSilTr7S7KA3C8caXFcHcM15tLggSzRbgtRZAWgPRIOM1WDNpltuy5zLMHN0CwsH8lQcI6ZsBWXwtgUEs1a08I8NM13%2BDB3RIlCO55FKjwn7MUAVYB%2FON9Fqrzk8fHpTISBK1kXEUrWVLV2hMWPqVzvtUCRj5g%2BijkA6xZ%2BHcHMxhRGBO8L64qgt4RZKqYJ6uBXipTewL98y6haIPc0gdtIZOtPF9oMHDHRnJ3d19LWUSz7ur8kDAFHO0qjkgqVnvCwy1f%2FQwhpHlBKCYwbjAAsldRULiQgS9UhMdX2ArAYN5cCfzNZmO4UvA9Z%2B3YF%2BVqgOnbNOGyMMIUNjNlvvw%2FvPt8WyRw%2FAkftE7IjyVr04LZ68L%2BoomSHM3ye4z8zCc4BHPuVRD79JcG9J4k%2Bnax%2BPO4i9UJXVMDPOox8LDeOpcUul6bPtw28L9Z%2FJ6hvwQxe5HlnmvRpHsKYEs8j7A3Q3T7uq5oc4b13GMmxAh8ngXhDjVkGJpQrBA2Jikig9crrKekkHCvL1eU3uxV9Qr4gugXdlX7YNtmcf537sttqzif8%2FfWypO0cfIACQu62a7heb0O3qx%2BlK%2Fz3sUPod490qwmNef7Fy72VP%2Bq0ivv%2F%2BZiSW4bG%2FBOtjHmwCq3jfJ13ts2WnuXz9iG2P%2BMbYyN69pGOcW%2BX0fr4MChc8KBjyZrTRMcuHFJnXlFCQRhgDg6c5D462LH48Ccjm1Nbzj3xv71srimDWbGIFui%2FEWIC9WFLFV4yBKEuvFzVH7Mb%2FAw4PvhwxhEqSrJZI6PQ7VoHY6CJGFi%2Fnmfft0%2F79Kn8Jd%2Fvmm79Z0zNLo5g3LWvcoV1NbDyan73GEcF2Q%2FQ0rTwhrAjuLB0ZdQfkRkuBTNjaTu2dMJswQJzVRPNkU0nn82Np1VorM29rYGOroWoxgG2X6KJa6mRbPDW%2FZUSSaHJe7pFvKJKqN%2B06bu9Xcsv7GS4IOYH7ppwBMqYp6iVVw9O7GscGoFQbrc0GK2Tu8qQvfZ2nOd73WgwJKkEuIuWb3ERuVqgrc2%2Fn8crCJ82ATga3O9s5oA9bTItm10mxIX8SHMCUsNH5dt%2FeoVH9O%2FVjzem63vPnjY3YWdl3dkm%2FTPkWCo2KQPnirX1EGydwjTHtNUVchqh9xqND8%2B5nIoOgqWbgvxZ6Ew3hOKH7on9SwUP2qDKjutvy2dZzr1d87NxX8%3D%3C%2Fdiagram%3E%3C%2Fmxfile%3E) |
```text
+---------------------------+               +--------------------------------+
|          User             |               |       Airflow (8081)           |
| (Private Equity Analyst)  |               | dags/evidence_indexing_dag.py  |
+------------+--------------+               +---------------+----------------+
             |                                              |
             | (search, justify, IC prep)                   | (scheduled indexing)
             v                                              v
+---------------------------+               +--------------------------------+
|    Streamlit (8501)       | <-----------> |        FastAPI (8000)          |
| streamlit_app/app.py      |   REST calls  |   app.main:app + routers       |
+------------+--------------+               +---------------+----------------+
             |                                              |
             |                              +---------------+----------------+
             |                              |     CS4 Service Layer          |
             |                              |--------------------------------|
             |                              | src/services/search/           |
             |                              |   vector_store.py              |
             |                              | src/services/retrieval/        |
             |                              |   hybrid.py (Dense+BM25+RRF)   |
             |                              |   hyde.py (query enhancement)  |
             |                              |   dimension_mapper.py          |
             |                              | src/services/justification/    |
             |                              |   generator.py                 |
             |                              | src/services/workflows/        |
             |                              |   ic_prep.py                   |
             |                              | src/services/collection/       |
             |                              |   analyst_notes.py             |
             |                              | src/services/integration/      |
             |                              |   cs1_client.py                |
             |                              |   cs2_client.py                |
             |                              |   cs3_client.py                |
             |                              +---------------+----------------+
             |                                              |
             v                                              v
+---------------------------+               +--------------------------------+
|    ChromaDB (local)       |               |         Snowflake DB           |
|  chroma_data/             |               |   companies, signals,          |
|  Dense vector index       |               |   assessments, dimension       |
|  BM25 sparse index        |               |   scores, evidence             |
+---------------------------+               +--------------------------------+
                                                           ^
                                                           |
                                       +-------------------+------------------+
                                       |         Evidence Layer (CS2)         |
                                       |  app/pipelines/ + app/routers/       |
                                       |--------------------------------------|
                                       | SEC EDGAR → S3 → parse → chunk      |
                                       | board_collector.py (dynamic CIK)    |
                                       | patent_signals.py (dynamic USPTO)   |
                                       | leadership_signals.py (Wikidata)    |
                                       | glassdoor_collector.py              |
                                       | pipeline.py (on-demand onboarding)  |
                                       +--------------------------------------+

Storage:
- ChromaDB: dense vector index + BM25 sparse index for hybrid search
- Snowflake: companies, signals, dimension scores, evidence metadata
- AWS S3: raw/parsed SEC documents

Ports:
- FastAPI: 8000 | Streamlit: 8501 | Airflow: 8081
```

---

## 🔍 CS4 Core Components

### 1️⃣ Integration Layer

Connects CS4 to all upstream case studies.

**CS1 Client** — fetches company metadata (ticker, sector, market cap)
**CS2 Client** — loads evidence chunks with source type, confidence, dimension
**CS3 Client** — retrieves dimension scores, rubric criteria, level keywords
**Dimension Mapper** — maps evidence sources to the 7 Org-AI-R dimensions

---

### 2️⃣ Multi-Provider LLM Router (LiteLLM)

Routes LLM calls across providers with fallback:

```
Primary: Claude (Anthropic)
Fallback: GPT-4 (OpenAI)
```

Supports:
- Score justification generation
- IC meeting prep synthesis
- Analyst note summarization
- Evidence quality assessment

---

### 3️⃣ Hybrid Retrieval (Dense + BM25 + RRF)

**Dense retrieval** — ChromaDB with sentence-transformers embeddings (semantic similarity)

**BM25 sparse retrieval** — keyword-based exact matching

**RRF Fusion** — Reciprocal Rank Fusion combines both rankings for best results

```
Final Score = 1/(k + rank_dense) + 1/(k + rank_bm25)
```

Filters available:
- `company_id` — filter by ticker
- `dimension` — filter by AI readiness dimension
- `min_confidence` — minimum evidence confidence threshold
- `top_k` — number of results

---

### 4️⃣ HyDE Query Enhancement

HyDE (Hypothetical Document Embedding) improves retrieval by:

1. Taking the original query
2. Generating a hypothetical answer with LLM
3. Embedding the hypothetical answer
4. Using it to retrieve real evidence

Result: better semantic matching for complex PE questions.

---

### 5️⃣ Score Justification Generator

For any company + dimension combination, generates:

- Score and rubric level (1-5)
- 95% confidence interval
- Evidence strength (strong/moderate/weak)
- Rubric criteria matched
- Supporting evidence items with citations
- Gaps preventing a higher score
- LLM-generated IC-ready summary (150-200 words)

---

### 6️⃣ IC Meeting Prep Workflow

Generates a complete Investment Committee package:

- Portfolio Org-AI-R score
- Executive summary
- Key strengths (top 3)
- Key gaps (top 3)
- Risk factors
- Recommendation (PROCEED / PROCEED WITH CAUTION / DO NOT PROCEED)
- Dimension-by-dimension justifications

---

### 7️⃣ Analyst Notes Collector

Indexes post-LOI due diligence evidence directly from PE analysts.

Supported note types:
- **Interview transcripts** — CTO, CDO, CFO conversations (confidence = 1.0)
- **Management meeting notes** — executive sessions (confidence = 1.0)
- **Site visit observations** — on-the-ground findings (confidence = 1.0)
- **DD findings** — due diligence investigation results (confidence = 1.0)
- **Data room summaries** — document repository analysis (confidence = 0.9)

Each note is tagged with:
- Dimensions discussed (maps to 7 Org-AI-R dimensions)
- Key findings and risk flags
- Assessor identity (audit trail)

Analyst notes are indexed into ChromaDB alongside SEC filings and signals, making them searchable and citable in justifications. Primary source confidence (1.0) means they rank highest in evidence retrieval.

---

### 8️⃣ On-Demand Company Onboarding

`POST /api/v1/pipeline/onboard/{ticker}`

Automatically onboards any US-listed company in 5 steps:

1. Register in Snowflake (dynamic industry mapping)
2. Collect SEC 10-K filings via EDGAR
3. Collect signals (jobs, patents, board governance, leadership)
4. Run CS3 scoring pipeline
5. Index evidence into ChromaDB

Works for **any** US-listed ticker — not just the original 5.

Tested: MSFT (351 items, 64.0), AMZN (391 items, 61.4), JNJ (127 items, 56.1), AAPL (227 items, 67.0)

---

### 9️⃣ Dynamic Signal Collection

**Board Governance** — dynamic CIK lookup via SEC official ticker map for any company

**Patent Signals** — dynamic USPTO name resolution via SEC EDGAR + USPTO assignee API. Uses `_text_phrase` search instead of exact match.

**Leadership Signals** — 3-source enrichment:
- SEC DEF 14A proxy statements (board governance)
- Wikidata — current C-suite and board members for any public company
- Wikipedia — AI background detection from executive articles (detects PHD, AI company veteran, ML keywords)

---

## 📡 API Endpoints

### Search
```
GET /api/v1/search
  ?query=AI talent hiring machine learning
  &company_id=NVDA
  &dimension=talent
  &top_k=10
  &min_confidence=0.8
```

### Score Justification
```
GET /api/v1/justification/{ticker}/{dimension}
```

### IC Meeting Prep
```
POST /api/v1/justification/{ticker}/ic-prep
Body: { "focus_dimensions": ["data_infrastructure", "talent"] }
```

### On-Demand Onboarding
```
POST /api/v1/pipeline/onboard/{ticker}?sector=Technology
GET  /api/v1/pipeline/onboard/{ticker}/status
GET  /api/v1/pipeline/supported-sectors
```

---

## 🗄 Infrastructure & Persistence

### Vector Database
- ChromaDB (local, `./chroma_data`)
- Dense embeddings: `sentence-transformers/all-MiniLM-L6-v2`
- BM25 sparse index in-memory

### Database
- Snowflake (primary analytics storage)

### Document Storage
- AWS S3 for SEC filings & raw documents

### Orchestration
- Airflow DAG (`evidence_indexing_dag.py`)
- Batch evidence indexing automation

### Containerization
- Dockerfile + docker-compose.yml

---

## 📂 Project Structure

```bash
PE_ORGAIR_CASESTUDY4/
│
├── app/                          # FastAPI application layer
│   ├── pipelines/
│   │   ├── board_collector.py    # Dynamic CIK lookup (any company)
│   │   ├── patent_signals.py     # Dynamic USPTO name lookup
│   │   ├── leadership_signals.py # Wikidata + Wikipedia enrichment
│   │   └── ...
│   ├── routers/
│   │   ├── search.py             # Evidence search endpoint
│   │   ├── justification.py      # Score justification endpoint
│   │   ├── pipeline.py           # On-demand onboarding
│   │   └── signals.py            # Signal collection
│   └── services/
│
├── src/
│   ├── services/
│   │   ├── integration/          # CS1/CS2/CS3 clients
│   │   │   ├── cs1_client.py
│   │   │   ├── cs2_client.py
│   │   │   └── cs3_client.py
│   │   ├── retrieval/
│   │   │   ├── hybrid.py         # Dense + BM25 + RRF fusion
│   │   │   ├── hyde.py           # HyDE query enhancement
│   │   │   └── dimension_mapper.py
│   │   ├── search/
│   │   │   └── vector_store.py   # ChromaDB wrapper
│   │   ├── justification/
│   │   │   └── generator.py      # Score justification LLM
│   │   ├── workflows/
│   │   │   └── ic_prep.py        # IC meeting prep
│   │   └── collection/
│   │       └── analyst_notes.py  # Post-LOI due diligence notes
│
├── dags/
│   └── evidence_indexing_dag.py  # Airflow batch indexing
│
├── scripts/
│   └── index_evidence.py         # Dynamic evidence indexing (any ticker)
│
├── streamlit_app/
│   ├── app.py                    # Full dashboard + CS4 pages
│   └── api_client.py             # API client with CS4 methods
│
├── tests/                        # 81 passing tests
├── results/                      # Scoring outputs (NVDA, JPM, WMT, GE, DG)
│
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## ▶️ How to Run

### Install Dependencies

```bash
poetry install
poetry run scrapling install  # install browser for stealth scraping
```

### Environment Variables

```bash
# Required
ANTHROPIC_API_KEY=...
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_WAREHOUSE=...
USPTO_API_KEY=...

# Optional
OPENAI_API_KEY=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
REDIS_URL=...
```

### Index Evidence

```bash
# Index original 5 companies
poetry run python scripts/index_evidence.py

# Index any new company
poetry run python scripts/index_evidence.py AAPL
poetry run python scripts/index_evidence.py MSFT GOOGL TSLA
```

### Run Tests

```bash
poetry run pytest  # 81 tests, 1 skipped
```

### Run the Application

#### Start FastAPI Backend

```bash
poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API available at:
- http://localhost:8000
- Swagger Docs: http://localhost:8000/docs

#### Start Streamlit Dashboard

```bash
poetry run streamlit run streamlit_app/app.py
```

Streamlit available at:
- http://localhost:8501

#### Run Airflow

```bash
docker-compose up --build
```

Airflow available at:
- http://localhost:8081

---

## 🧪 Testing & Validation

- 81 unit and integration tests passing (1 skipped intentional)
- All 3 CS4 endpoints verified (Search, Justification, IC Prep)
- All 5 original tickers × 7 dimensions = 35 combinations tested
- New companies tested: MSFT (351 items), AMZN (391 items), JNJ (127 items), AAPL (227 items)
- Evidence indexed: NVDA (435), JPM (1,756), WMT (317), GE (369), DG (797)

---

## 👥 Team Contributions

### Ayush Fulsundar
- CS1/CS2/CS3 Integration clients
- Hybrid retrieval (Dense + BM25 + RRF)
- HyDE query enhancement
- Vector store (ChromaDB)
- Evidence indexing pipeline
- Airflow evidence indexing DAG

### Ishaan Samel
- Score Justification Generator
- IC Meeting Prep Workflow
- Analyst Notes Collector
- LiteLLM multi-provider router
- Justification API endpoint
- Streamlit CS4 pages (Evidence Search, Score Justification, Analyst notes)

### Vaishnavi Srinivas
- On-demand company onboarding pipeline
- Dynamic CIK lookup for board governance
- Dynamic USPTO name resolution for patents
- Wikidata + Wikipedia leadership enrichment

---

## 🤖 AI Usage Disclosure

AI tools used during development:

- ChatGPT — debugging, architectural refinement, documentation structuring
- Claude — debugging support, structured test refinement, implementation assistance

---

## 📜 License

Academic project for QuantUniversity — Spring 2026
