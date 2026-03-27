# PE Org-AI-R Platform
## Case Study 5 — Agentic Portfolio Intelligence
### "Claude, prepare the IC meeting for NVIDIA."

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
| Demo Video | [[Demo Video](#)](https://github.com/BigDataIA-Spring26-Team-03/PE_OrgAIR_CaseStudy4.git) |
| Interactive Codelab | [CS5 Agentic Portfolio Codelab](https://codelabs-preview.appspot.com/?file_id=1qHfY9Cs6SipX6US_RT8QELEkXO79qfaO5GIZLKRGJLc#0) |
| Deployed FastAPI Link | |
| Deployed App Link |  |

---

## 📌 Executive Summary

Case Study 5 is the capstone of the PE Org-AI-R platform, transforming the CS1-CS4 evidence and scoring pipeline into a fully agentic investment intelligence system.

Building on:

- Case Study 1 → Platform Foundation (FastAPI, Snowflake, Redis, Docker)
- Case Study 2 → Evidence Collection & Signal Extraction
- Case Study 3 → Risk-adjusted, sector-calibrated scoring engine
- Case Study 4 → RAG-powered search and score justification
- **Case Study 5 → Agentic orchestration via MCP + LangGraph**

The CS5 layer answers the PE firm's core workflow question:

> *"Claude, prepare the IC meeting for NVIDIA."*

It wraps all CS1-CS4 APIs as MCP tools and orchestrates them with LangGraph specialist agents to produce full agentic due diligence workflows, IC meeting packages, and portfolio-level Fund-AI-R analytics — all backed by real data, zero mock data.

Key capabilities:

- MCP Server exposing 6 CS1-CS4 tools to any LLM agent
- LangGraph multi-agent supervisor with SEC, Scoring, Evidence, and Value Creation specialists
- Human-in-the-Loop (HITL) approval gates for scores outside normal range
- Assessment History Tracking with trend analysis
- Fund-AI-R Calculator using EV-weighted aggregation
- Prometheus metrics for observability
- Portfolio Dashboard via Streamlit

---

## 🏗 System Architecture Overview

| Architecture Diagram | [Draw.io Diagram](https://viewer.diagrams.net/?tags=%7B%7D&lightbox=1&highlight=0000ff&edit=_blank&layers=1&nav=1&title=CS5&dark=auto#R%3Cmxfile%3E%3Cdiagram%20name%3D%22Page-1%22%20id%3D%228ICr4UUGzuBkgfGW4hdd%22%3E5Vxbc9q6Fv41zHQ%2FtOMLNvAIhKQ5k7ZMyLRnPzHCFkY7xmYkAcn59XtJFr7IJhBjSJiTThNblo20vnX5lrRMyx4uX%2B4oWi1%2BxD4OW5bhv7Tsm5ZlmWanB39Ey2vS0rFUQ0CJrzplDRPyP6waDdW6Jj5mhY48jkNOVsVGL44i7PFCG6I03ha7zeOw%2BKkrFOBSw8RDYbn1D%2FH5QrW6Tju78B2TYLH7aNNVE1yiXW81FbZAfrzNNdmjlj2kccyTo%2BXLEIdCejvBJPfd7rmajoziiB9zAyccZlW6ST2H8dfdnDl%2BgWuDBV%2BG0GDCIeM0fsbDOIwptERxBD0HcxKGWhMKSRDBqQdPx9A%2B2GDKCUizry4sie%2BLjxlsF4TjyQp54jO3oDvQRuN15GMxWkM8Po640gcrPVeDFGNKhr1B4VoNezyC8180%2BNq%2F%2F%2FrYsvtwNkQMZmxM%2BBr0zzIc%2BN8aWa2u0eoBfkY%2FgHESD47GMeWgGySG43sYewjjxZGnxCVmgV9y4lICvsPxEnMqHr3I6UBbAb7NKYxlqEb1mPT8dXeudFbpa5A%2BOkMUDhSo1QCvGcj8CHxBEVficPkSCKP9Ng%2FjrbdAlH9jHH5PLQ3clmX7CHfnXkkT4IrrdfFsXsTLtI7Gqx%2Bh8JXxWnJ2K%2BSsibnjFMXcbUDMIAKMliHh75E1BeeEomCv7ueMTRe9g7t%2Bu0r0XWtmu64m%2BkpRT7IhGzeILWYxon7LckNh5zNQGjfg8r7DLXlL%2BQWAbQjelnvdgiUndigU22h1hsnB75YzEm0bBr%2B%2BqxNwt1y4i9JTRhtw%2FtIKjRvCViESGjCMlyvwNlE9nTHNstLY7QO2aTagNXMExrUi16MztzDg%2FvgeGgfIe8bRG%2Frik02VwtzqjdBU6NoQfmaviJ9r6fiZp%2BO3ouKeBV6zy0A4n88tr9Lj%2Bu7MdY6CcJwbs%2FEDZkw8OHLhGaaIqExYrwfsyrqNZ%2BIEzQj4iFchiqTzt9Vrrn%2FucOmtpoKHTSG2hwwOOQpzl5EIrFMSbWIwbRJH5R4gi3CKViBVGG96uYZCpMaaU4g0MqQ0VLNos4lwK2SwwMh%2FX9B9j0bk8LUrIqpOrni8qtAjR%2FxTd%2Bfak5%2FKiC5%2FqvTpx3As3PXo8bf023kqBcIAeQklwlRqTUNQmrpzNjXjtpsI6byCTDcOoVEl0qdfvx5yZgFoeusQcTyNaTBFhE6ZF1NcbYSTeE3F50qmO7FzVx4xX9OIJZdSSmwNswg8zOJvDaS6FUB1nTeDaNt2GsDJ%2BiQ4BZhPPSAiKHqd4pSlHAbJ2gdS9hDDW6yjZ3YuXNqGU8Sl3QQu9gfjohETCD%2BYCiv6Z804mZMkCh1Db4twtT%2BA4RyBoU5w2u0mfGD7c2EI1OAf%2BJApnhHuoykBY%2FP4%2ByEEzTRCoCH12EUl3dTQ6FqaRTVBLrjzSTwdXUfTAK2mSCbqhB0ZigaJ8VT6OmBjX32Zy0FKF9XCpXcYFtOwNVwaiUDuJ8FFRKDVLhefsvVyiegesl4Ex9wHSy5nP4%2FbMq22jkgDlhIGZ2bhe%2Fj0ft5ck7fv4eclRXjo%2F7y7e%2ByPv0Nz%2F27082micXGZfTEZneq4vCokbaeUURWQdJuIQGy9Ehkpiy8F5Ry7e1LsTm9mGMdY5SQ35mL6KwDIZrQ3k36M11yk4MYM8y3GUQ6%2FtM%2BGoJbc4vCJ4DEilhnYD3A9kmhWpVldDd%2Behm%2B3Ad%2FJsDdFVwTtSCRJ%2FTTuJdsF1SgOxTJI4kYPpAbDGOCSm06is484UJtoTpFMyTLvLnpFSN4qRsGxt5gyjrznBmlMEfK2btKNQC6y16sCHQZMouBotA8n7E%2BUBAGmcsH7%2FulBCGcuZq51AzfAQFmEK3cGwpsK2AEC5%2BZsRq6nEW6vASeON9cEd26HYYf3wfyiYOtv5ZmXXBa3DsPdtXUDbwBukOU14f1bHgINplhBVQv3iqTo4ogfE8WN9hkgF0v314O48rl9tdUAh3dgsNV%2BfYzWTHIxuS%2Beu%2FAHEUHIjLmgecZivUSSp6XPzFKtxKsbO2hFDDXkTTvoBSJp99Hg%2Fummn%2BsP3Z2zRXjT0nbD3U4D6kAi%2Fn%2BWgN3%2FfBpBAvZ0%2F%2BsnXHjo%2F13aDcntrNVD067KwbRtzk4RzG4Ttr3yP%2Bf2ZqVp50sRboBIw59JIvg8VxMLHgO58j7IrUwZ6wjiNfZzPUGRMRh%2BEhZus7UVydEVonW3tipy6rTkZ8%2BGSa%2BJqoMFYRcqU2kC0D4D98uWMh4b32HosVzS0uLwE5XFCAankA4lXP2LbfjSi4K5GT3D%2F%2BsDAnKnAmKzCHFbC8e9JlaK55B0XA%2FE%2BcKgocqdCmsno99ft1KqwjYNFAQUBztWna6vCNrFZTHD7RweKPKuBi2zlCf1zgBbCCHzemB7kAGeLchKOFmImihkBdoDgwgTWwQPC7%2F%2FkGeywr5cuHocTZ5ynQdJ8ZnxRThcCk%2F6K71LPkcC%2FwW%2FpBcbQlULnz3zDP5W3HA9qB6oD0rKPRJkVJZkcBxiJZVs6eKN4qGGHGmpoMc9g0l67GxlICdV06bbQ29V0xY3dsbgViFZWUKfW1FVrXvQrLhPGV4Ub%2BchesZpyyMYb2NFCNaBCkvTaCIT8Zh1zfhZ%2BSiYrUvB00MYaRG%2FZG06QWoM%2BazcL1Dn%2FwF7TE%2BUsz0PjI51HhjPVk1yCRhtbTkgiYqjKCBRPi%2FJqrIUUrvS6OSsfm3WETy06%2Bq4NZE7euxsFSSXwK1QrtC%2Fk8kkol5%2BGegGRyzzkIMflpO5y8d6mX6FldmWHuycc5gZy1z%2B8aB5r8DygAna5162qd6cyYacVUzKrTYi1lqES2QlborSpHLXRazR1dw6rao70bhJu1xr3IRxLYCrIX92PWAN5YhvBgfsJ72Yq4DcgGuIaU2Iqt6%2B0uJWp30OiN4Xtj4WnP4fUTRSrBlGW9EkiQV8IoStvBXBpJhMzPzYWytjqoOOc9iAsmjUKDxYYSEKJwrgVOGlKqZEU%2FZWXQ5F8ZBdPIkpX8SBKMsYZa0D%2FEL4f%2BGq8c1RZ38XYg9HNMDqUwtvlJUFSDGwebIpjvokUVifVhS516ROEIS8tU%2BpLDLcdVjFRKmtevJYNORYke64O05eqKX%2BHZ1FFfvDQTKCDJJ0KsehZNdDSXs%2F8V1QlQEpvuVyCeVs15t24QW7q5u0U2%2FS%2BjtIJ867UEt5iWm79aat1XyeOOviDuYlpt2pN219q%2FXEeav1rktMuPtJJmxdasK9TzJh%2B1IT3i0xfPiM2xebcU1CmS4znzjVQgJ%2FkQnXpI3pwtCp2OZy4EuTQ9PSkxJTLcLso4ddfXFUu%2BF0fmjWJIjpOvmpCniqc6mFhKsn77v8cR8Qpc0i%2FY4GkKjJWU%2Bibz5iC%2FltOeVv6MnvApZxK36VwUU8R016ewrP%2B3jx1MtC9ffb7QPabdlv3vBe5YbT7Cupku7ZN3vZo38B%3C%2Fdiagram%3E%3C%2Fmxfile%3E#%7B%22pageId%22%3A%228ICr4UUGzuBkgfGW4hdd%22%7D) |
|---|---|

```text
+---------------------------+               +--------------------------------+
|          User             |               |     Prometheus Metrics         |
| (Private Equity Analyst)  |               |  services/observability/       |
+------------+--------------+               +---------------+----------------+
             |                                              |
             | (IC prep, due diligence, portfolio)          | (observability)
             v                                              v
+---------------------------+               +--------------------------------+
|    Streamlit (8501)       | <-----------> |        FastAPI (8000)          |
| dashboard/app.py          |   REST calls  |   app.main:app + routers       |
+------------+--------------+               +---------------+----------------+
             |                                              |
             |                              +---------------+----------------+
             |                              |       MCP Server Layer         |
             |                              |--------------------------------|
             |                              | pe_mcp/server.py               |
             |                              |  Tools:                        |
             |                              |   calculate_org_air_score      |
             |                              |   get_company_evidence         |
             |                              |   generate_justification       |
             |                              |   project_ebitda_impact        |
             |                              |   run_gap_analysis             |
             |                              |   get_portfolio_summary        |
             |                              |  Resources: parameters, sectors|
             |                              |  Prompts: due_diligence, ic    |
             |                              +---------------+----------------+
             |                                              |
             v                                              v
+---------------------------+               +--------------------------------+
|   LangGraph Agents        |               |   Integration Layer            |
|  agents/supervisor.py     |               |--------------------------------|
|  agents/specialists.py    |               | services/integration/          |
|  agents/state.py          |               |   portfolio_data_service.py    |
|                           |               | services/tracking/             |
|  Specialist Agents:       |               |   assessment_history.py        |
|  - SECAnalysisAgent       |
|  - TalentAnalysisAgent    |               | services/analytics/            |
|  - ScoringAgent           |               |   fund_air.py                  |
|  - EvidenceAgent          |               | services/observability/        |
|  - ValueCreationAgent     |               |   metrics.py                   |
|  HITL Approval Gate       |               +---------------+----------------+
+---------------------------+                              |
                                                           v
                                       +-------------------+------------------+
                                       |         CS1-CS4 Integration          |
                                       |  services/integration/cs1_client.py  |
                                       |  services/integration/cs2_client.py  |
                                       |  services/integration/cs3_client.py  |
                                       |  services/cs4_client.py              |
                                       +--------------------------------------+
                                                           |
                                                           v
                                       +-------------------+------------------+
                                       |         Snowflake DB + ChromaDB      |
                                       |  companies, signals, assessments     |
                                       |  dimension scores, evidence          |
                                       |  Dense vector + BM25 index           |
                                       +--------------------------------------+

Ports:
- FastAPI: 8000 | Streamlit: 8501 | MCP Server: stdio
```

---

## 🔍 CS5 Core Components

### 1️⃣ MCP Server (Lab 9)

Wraps all CS1-CS4 APIs as MCP tools that any LLM agent can invoke.

**6 Tools exposed:**
- `calculate_org_air_score` — CS3 scoring engine → Org-AI-R, V^R, H^R, dimension breakdown
- `get_company_evidence` — CS2 evidence retrieval → filtered by dimension
- `generate_justification` — CS4 RAG justification → score, level, evidence, gaps
- `project_ebitda_impact` — EBITDA projection model v2.0 → conservative/base/optimistic scenarios
- `run_gap_analysis` — CS3+CS4 gap analysis → 100-day improvement plan
- `get_portfolio_summary` — CS1 portfolio → Fund-AI-R + company breakdown

**2 Resources:**
- `orgair://parameters/v2.0` — scoring parameters (alpha, beta, gamma values)
- `orgair://sectors` — sector baselines and weights

**2 Prompts:**
- `due_diligence_assessment` — complete DD workflow template
- `ic_meeting_prep` — IC package generation template

---

### 2️⃣ Portfolio Data Service

Unified integration layer between CS1-CS4 and the MCP server.

- Loads portfolio companies from CS1
- Fetches Org-AI-R scores from CS3 for each company
- Retrieves evidence counts from CS2
- Builds `PortfolioCompanyView` with delta tracking

---

### 3️⃣ LangGraph Multi-Agent System (Lab 10)

**4 Specialist Agents:**

| Agent | Responsibility | CS Integration |
|---|---|---|
| SECAnalysisAgent | SEC filing analysis | CS2 evidence |
| TalentAnalysisAgent | External signal analysis | CS2 evidence |
| ScoringAgent | Org-AI-R calculation + HITL trigger | CS3 scoring |
| EvidenceAgent | Dimension justifications | CS4 RAG |
| ValueCreationAgent | EBITDA projection + gap analysis | CS3+CS4 |

**Supervisor** routes between agents using LangGraph conditional edges.

**HITL Approval Gates** trigger when:
- Org-AI-R score > 85 or < 40
- EBITDA projection > 5%

---

### 4️⃣ Assessment History Tracking

Stores point-in-time assessment snapshots for trend analysis.

- `record_assessment()` — stores current CS3 scores to Snowflake
- `get_history()` — retrieves snapshots filtered by date range
- `calculate_trend()` — computes delta_30d, delta_90d, trend direction (improving/stable/declining)

---

### 5️⃣ Fund-AI-R Calculator

Aggregates company-level Org-AI-R into a portfolio-level metric.

- EV-weighted Org-AI-R averaging
- Sector HHI concentration metric
- Quartile distribution across sector benchmarks
- AI leaders (≥70) and laggards (<50) counts

---

### 6️⃣ Prometheus Metrics

Real-time observability for MCP tools and LangGraph agents.

- `mcp_tool_calls_total` — per tool, per status
- `mcp_tool_duration_seconds` — execution latency histogram
- `agent_invocations_total` — per agent, per status
- `hitl_approvals_total` — approval reason + decision
- `cs_client_calls_total` — CS1-CS4 service call tracking

---

### 7️⃣ Bonus Extensions

| Extension | Points | Description |
|---|---|---|
| Mem0 Semantic Memory | +5 pts | Persistent agent memory across sessions |
| Investment Tracker with ROI | +5 pts | Track portfolio ROI over time |
| IC Memo Generator | +5 pts | Word document IC memo generation |
| LP Letter Generator | +5 pts | Limited partner letter automation |

---

## 📡 API Endpoints

### MCP Tools (via stdio server)
```
calculate_org_air_score   {"company_id": "NVDA"}
get_company_evidence      {"company_id": "NVDA", "dimension": "talent"}
generate_justification    {"company_id": "NVDA", "dimension": "leadership"}
project_ebitda_impact     {"company_id": "NVDA", "entry_score": 55, "target_score": 75, "h_r_score": 80}
run_gap_analysis          {"company_id": "NVDA", "target_org_air": 80}
get_portfolio_summary     {"fund_id": "growth_fund_v"}
```

### REST Endpoints (FastAPI)
```
POST /api/v1/pipeline/onboard/{ticker}        On-demand company onboarding
GET  /api/v1/signals/summary                  Portfolio signal summary
GET  /api/v1/justification/{ticker}/{dim}     Score justification
POST /api/v1/justification/{ticker}/ic-prep   IC meeting package
```

---

## 🗄 Infrastructure & Persistence

### Databases
- **Snowflake** — companies, signals, assessments, dimension scores
- **ChromaDB** — dense vector + BM25 sparse index for evidence retrieval

### Document Storage
- **AWS S3** — raw SEC filings and parsed documents

### Orchestration
- **LangGraph** — multi-agent supervisor with HITL checkpointing (MemorySaver)
- **MCP Server** — stdio transport, compatible with Claude Desktop and any MCP client

### Containerization
- **Dockerfile + docker-compose.yml**

---

## 📂 Project Structure

```bash
PE_ORGAIR_CASESTUDY5/
│
├── app/                              # FastAPI application
│   ├── pipelines/
│   │   ├── leadership_signals.py     # Scrapling + Wikipedia enrichment (real, no mock)
│   │   ├── board_collector.py        # Dynamic CIK lookup
│   │   └── patent_signals.py         # Dynamic USPTO name resolution
│   ├── routers/
│   │   ├── signals.py                # Signal collection (board + scraped)
│   │   └── pipeline.py               # On-demand onboarding
│   └── services/
│
├── src/
│   └── services/
│       ├── integration/
│       │   ├── portfolio_data_service.py   # CS1-CS4 unified interface
│       │   ├── cs1_client.py
│       │   ├── cs2_client.py
│       │   ├── cs3_client.py
│       │   └── cs4_client.py
│       ├── tracking/
│       │   └── assessment_history.py       # Trend tracking
│       ├── analytics/
│       │   └── fund_air.py                 # Fund-AI-R calculator
│       └── observability/
│           └── metrics.py                  # Prometheus metrics
│
├── pe_mcp/
│   └── server.py                           # MCP Server (6 tools, 2 resources, 2 prompts)
│
├── agents/
│   ├── state.py                            # LangGraph DueDiligenceState
│   ├── specialists.py                      # 4 specialist agents
│   └── supervisor.py                       # Supervisor + HITL graph
│
├── dashboard/
│   ├── app.py                              # Streamlit portfolio dashboard
│   └── components/
│       └── evidence_display.py             # Evidence card component
│
├── exercises/
│   └── agentic_due_diligence.py            # End-to-end DD workflow
│
├── tests/
│   └── test_mcp_integration.py             # CS1-CS4 integration tests
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
poetry run python -m patchright install chromium   # for leadership scraping
```

### Environment Variables

```bash
# Required
ANTHROPIC_API_KEY=...
OPENAI_API_KEY=...
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_WAREHOUSE=...
USPTO_API_KEY=...

# Optional
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
REDIS_URL=...
```

### Start FastAPI Backend

```bash
poetry run uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Start Streamlit Dashboard

```bash
poetry run streamlit run streamlit_app/app.py
```

### Run MCP Server

```bash
poetry run python mcp/server.py
```

### Run Agentic Due Diligence

```bash
poetry run python exercises/agentic_due_diligence.py
```

### Run Tests

```bash
poetry run pytest tests/ -v --tb=short
```

---

## 🧪 Testing & Validation

- MCP tools verified to call CS1-CS4 clients (not hardcoded data)
- HITL triggers verified for scores outside [40, 85] range
- All 5 portfolio companies × 7 dimensions tested
- Leadership signals: NVDA=87, JPM=38, WMT=42, GE=20, DG=25 (board + scraped)
- Fund-AI-R calculated for growth_fund_v across all 5 tickers

---

## 👥 Team Contributions

### Ayush Fulsundar
- CS1/CS2/CS3/CS4 integration clients
- MCP server tool implementation
- MCP Resources and prompts
- Portfolio data service

### Ishaan Samel
- LangGraph specialist agents (SEC, Scoring, Evidence)
- Agentic due diligence workflow
- Portfolio dashboard (Streamlit)
- Fund-AI-R calculator
- IC Memo Generator + LP Letter Generator (bonus)
- Prometheus metrics

### Vaishnavi Srinivas
- Assessment history tracking
- Investment Tracker with ROI (bonus)
- Mem0 semantic memory (bonus)

---

## 🤖 AI Usage Disclosure

AI tools used during development:

- ChatGPT — debugging, architectural refinement, documentation structuring
- Claude — debugging support, implementation assistance, codelab generation

---

## 📜 License

Academic project for QuantUniversity — Spring 2026
