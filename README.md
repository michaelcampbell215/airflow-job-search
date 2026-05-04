# AI-Powered Job Search Pipeline

I built this to solve my own job search. Instead of
manually searching job boards and guessing which roles
fit my background, I automated the entire process from
resume analysis to a ranked shortlist delivered to my
Google Sheet.

Two Gemini AI agents drive the pipeline. The Analyzer
reads my resume and generates targeted search queries.
The Strategist scores each result 1–10 against my
background with a one-sentence explanation per job.
Airflow orchestrates everything from API extraction
through BigQuery transformation to final export.

---

## Architecture

```
Resume (Markdown/PDF)
│
▼
[1] Analyzer Agent (Gemini)
→ Generates 3–5 targeted search queries
│
▼
[2] JSearch API Extract
→ Pulls raw job listings for each query
│
▼
[3] BigQuery Load
→ Appends raw API responses to job_data.search_queries
│
▼
[4] BigQuery Transform
→ Flattens + deduplicates into job_data.raw_jobs
│
▼
[5] Strategist Agent (Gemini)
→ Scores each job 1–10 against resume
→ Saves scored results to job_data.ranked_job_search
│
▼
[6] Google Sheets Export
→ Writes top 100 ranked jobs to "AI Job Search" spreadsheet
```
---

## Key Engineering Decisions

**AI agents as markdown system prompts**
Both Gemini agents (Analyzer and Strategist) live in
`include/agents/` as `.md` files rather than hardcoded
strings. Prompts are version-controlled, diff-able, and
editable without touching Python — the same philosophy
as dbt's SQL-first approach to transformation logic.

**Airflow Params as a pipeline UI**
The DAG uses typed `Param` inputs with enums, min/max
constraints, and null handling to generate a trigger
form in the Airflow UI. Resume file, location, work
type, and page depth are all configurable at runtime
without touching code — non-technical users can run
the pipeline through the UI alone.

**Manual query override for AI fallback**
If the Analyzer generates poor queries, the
`manual_queries` param bypasses the AI step entirely
and accepts comma-separated job titles directly.
Defensive design for an AI-dependent pipeline —
the system degrades gracefully when the AI layer
underperforms.

**XCom for decoupled task handoff**
Search queries flow from the Analyzer task to the
extract task via Airflow XCom push/pull rather than
global state. Tasks are independently testable and
the data handoff between AI and API layers is
explicit and auditable in the Airflow UI.

**Run ID isolation for concurrent execution safety**
Each DAG run injects Airflow's `run_id` into every
scored job record before BigQuery insertion. The
Google Sheets export filters `WHERE airflow_run_id =
current_run_id`, guaranteeing each export contains
only results from that specific execution. Prevents
data cross-contamination when running the pipeline
multiple times with different resumes or locations.

**Two-stage BigQuery landing**
Raw API responses land in `search_queries` before
flattening into `raw_jobs`. Preserves the original
API payload for debugging and reprocessing without
re-hitting the API — a Medallion architecture pattern.

**Scoring threshold in the agent, not the pipeline**
The score 5 filter is enforced in the Strategist
system prompt, not in a downstream SQL filter.
Ranking logic lives in one place and is auditable
by reading a single markdown file.

**Pytest coverage across three test layers**
`test_dag_integrity.py` verifies DAG structure without
spinning up Airflow. `test_pipeline_functions.py` unit
tests ETL helpers with mocked GCP and API calls.
`test_live_api.py` runs integration tests against the
live JSearch API — marked `@pytest.mark.live` and
skipped by default so the standard test suite runs
without API credentials. All three layers run without
the full Docker stack.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.2.1 (Docker / Celery) |
| AI Agents | Google Gemini 2.5 Flash (`google-genai`) |
| Data Lake | Google BigQuery |
| Job Data | JSearch API via RapidAPI |
| Export | Google Sheets via `gspread` |
| Auth | Application Default Credentials (ADC) |

---

## Project Structure
```
├── dags/
│   ├── job_search_ai_dag.py        # Production DAG (6-task pipeline)
│   └── pipeline_functions.py       # Production helper functions
├── plugins/
│   ├── gemini_agent.py             # Gemini API wrapper (Analyzer + Strategist)
│   └── google_sheets_manager.py    # gspread wrapper
├── include/
│   └── agents/
│       ├── analyzer.md             # System prompt: resume → search queries
│       ├── strategist.md           # System prompt: jobs → ranked JSON
│       └── archivist.md            # System prompt: output formatting (planned)
├── data/
│   └── resumes/                    # Place .pdf or .md resumes here
├── tests/
│   ├── test_dag_integrity.py       # Pytest: DAG parse + structure checks
│   ├── test_pipeline_functions.py  # Pytest: unit tests for pipeline helpers
│   └── test_live_api.py            # Pytest: live API integration tests (skipped by default)
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```
---

## Setup

### Prerequisites

- Docker Desktop
- Google Cloud project with BigQuery and Google Sheets APIs enabled
- RapidAPI account with JSearch subscribed
- Google Gemini API key

### 1. Clone and configure

```bash
git clone <repo-url>
cd airflow-job-search
```

Copy your resume into `data/resumes/` as a `.md` or `.pdf` file.

### 2. Authenticate with Google Cloud

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

This generates credentials that Airflow picks up automatically
inside Docker via the mounted volume in `docker-compose.yaml`.

### 3. Start Airflow

```bash
docker compose up -d
```

Open the Airflow UI at `http://localhost:8080`
(default login: `airflow` / `airflow`).

### 4. Add Airflow Variables

Go to **Admin → Variables** and add:

| Key | Value |
|---|---|
| `RAPIDAPI_KEY` | Your RapidAPI key for JSearch |
| `GEMINI_API_KEY` | Your Google Gemini API key |
| `GOOGLE_SHEET_ID` | The ID from your Google Sheet URL (`/d/<ID>/edit`) |
| `GOOGLE_SHEETS_SA_KEY` | Full JSON contents of your Google Sheets service account key |

> **Note on `GOOGLE_SHEETS_SA_KEY`**: ADC user credentials do not
> include Google Sheets/Drive scopes by default and Google blocks
> adding them without a verified OAuth consent screen. The workaround
> is a dedicated service account: create one in **IAM & Admin →
> Service Accounts**, download the JSON key, paste the full JSON as
> this variable, and share your "AI Job Search" spreadsheet with the
> service account's `client_email` as Editor.

### 5. Run the pipeline

Trigger the `ai_portfolio_job_search` DAG. A form will appear
with configurable parameters:

| Parameter | Description |
|---|---|
| `resume_filename` | File in `data/resumes/` to analyze |
| `search_location` | City/region (e.g. `Tampa, FL, USA`) |
| `location_type` | `Any`, `Remote`, `Hybrid`, or `On-site` |
| `pages_per_query` | API pages per query (1 page ≈ 10 jobs) |
| `manual_queries` | Override AI queries with comma-separated titles |

---

## Running Tests

```bash
# From inside the Airflow Docker container or a local venv:
pytest tests/
```

`test_dag_integrity.py` — verifies DAG files parse without errors
and task structure is correct. No Airflow runtime required.

`test_pipeline_functions.py` — unit tests for ETL helpers using
mocked GCP and API calls.

### Live API Integration Tests

Tests marked `@pytest.mark.live` are skipped in the standard suite.
To run them explicitly against the live JSearch API:

```bash
# Set RAPIDAPI_KEY in a .env file at the project root, then:
pytest tests/test_live_api.py -v -s -m live
```

The `-s` flag prints API response details to the console.
These tests mock the Airflow Variable layer so no Airflow
database connection is required.

---

## BigQuery Tables

| Table | Description |
|---|---|
| `job_data.search_queries` | Raw API responses (one row per query run) |
| `job_data.raw_jobs` | Flattened, deduplicated job records |
| `job_data.ranked_job_search` | Gemini-scored jobs with `match_score`, `strategic_why`, and `airflow_run_id` |

---

## Sample Output

Top results exported to Google Sheets on each run.
Scores below 5 are filtered by the Strategist agent
before export.

| Job Title | Company | Company Website | Location | Work Location | Posted Date | Apply Link | Match Score | Why It Fits |
|---|---|---|---|---|---|---|---|---|
| Analytics Engineer | BayCare Health System | baycare.org | Tampa, FL | On-site | 2026-05-01 | [Apply](#) | 9 | Strong dbt and BigQuery alignment with healthcare data focus |
| Data Engineer | HCA Florida | hcahealthcare.com | Tampa, FL | Hybrid | 2026-05-01 | [Apply](#) | 8 | Airflow and Docker stack match JD requirements directly |
| Healthcare Data Analyst | Moffitt Cancer Center | moffitt.org | Tampa, FL | On-site | 2026-04-30 | [Apply](#) | 8 | CMS Open Payments experience maps to research compliance needs |
| Analytics Engineer | Tampa General Hospital | tgh.org | Tampa, FL | On-site | 2026-04-30 | [Apply](#) | 7 | Star Schema and ELT pipeline experience matches posted requirements |
| Data Engineer II | AdventHealth | adventhealth.com | Tampa, FL | Hybrid | 2026-04-29 | [Apply](#) | 6 | Pipeline orchestration experience relevant, Epic EHR familiarity gap noted |

*Company names, scores, and links are illustrative.
Actual output varies by resume and search run.*

---

## AI Agents

Both active agents live in `include/agents/` as Markdown system
prompts, making them editable without touching Python code.

**Analyzer** (`analyzer.md`) — reads the resume and outputs 3–5
targeted job search queries as a JSON array.

**Strategist** (`strategist.md`) — scores each job 1–10 against
the resume. Only jobs scoring 5 or above are returned. Each result
includes a one-sentence `strategic_why` explanation and an
`airflow_run_id` for export isolation.

**Archivist** (`archivist.md`) — a third agent for output
formatting and Google Sheets structuring. Defined and prompt-ready;
integration into the pipeline is planned.

---

## Required Google Cloud APIs

Enable these in your GCP project:

- BigQuery API
- Google Sheets API
- Google Drive API
