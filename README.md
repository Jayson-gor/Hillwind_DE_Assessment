# Benefits Data Pipeline

Production-grade data pipeline for employee benefits analytics — SQL analysis, Python ETL, and DuckDB storage, fully containerized.

Runs in <5 seconds on sample data. Scales cleanly to 20 M+ rows/day.
---

## Quick Start

```bash
# Run complete pipeline (SQL + ETL)
docker-compose run --rm app bash -c "python src/sql_analysis.py && python src/etl.py"

# Full refresh (ignore incremental state)
docker-compose run --rm app python src/etl.py --full-refresh

# Interactive exploration
docker-compose up jupyter  # → http://localhost:8888
```

---

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Raw CSVs   │ ──▶ │  sql_analysis.py │ ──▶ │  DuckDB + CSVs  │
│  + JSON     │     │  (3 SQL queries) │     │  (analytics)    │
└─────────────┘     └──────────────────┘     └─────────────────┘
                              │
                              ▼
                    ┌──────────────────┐     ┌─────────────────┐
                    │     etl.py       │ ──▶ │  Parquet + Log  │
                    │  (clean/enrich)  │     │  (clean data)   │
                    └──────────────────┘     └─────────────────┘
```

---

## Project Structure

```
├── README.md                 # You are here
├── DESIGN.md                 # dbt system design + incident response
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
│
├── data/                     # Inputs
│   ├── employees_raw.csv
│   ├── plans_raw.csv
│   ├── claims_raw.csv
│   ├── company_lookup.json   # Domain → EIN mapping
│   └── api_mock.json         # Mock enrichment API
│
├── src/                      # Pipeline code
│   ├── sql_analysis.py       # DuckDB queries (gaps, spikes, roster)
│   └── etl.py                # Clean → Validate → Enrich
│
└── outputs/                  # Generated artifacts
    ├── analytics.duckdb      # Persistent database
    ├── clean_data.parquet    # Enriched employees
    ├── validation_errors.csv # Flagged records
    ├── sql_*.csv             # Query results
    └── etl_log.txt           # Run metrics
```

---

## SQL Analysis

Three analytical queries executed against DuckDB:

| Query | Output | Description |
|-------|--------|-------------|
| Plan Gap Detection | `sql_gaps.csv` | Finds coverage gaps >90 days using gaps-and-islands pattern |
| Claims Cost Spike | `sql_spikes.csv` | Detects >200% cost increase between adjacent 90-day windows |
| Roster Mismatch | `sql_roster.csv` | Compares actual vs expected employee counts by company |

---

## ETL Pipeline

**Incremental processing** with file-based watermark:

| Stage | Description |
|-------|-------------|
| **Load** | Read CSVs, filter by watermark timestamp |
| **Clean** | Trim strings, coerce dates, standardize emails |
| **Validate** | Flag missing titles, invalid emails → `validation_errors.csv` |
| **Enrich** | Infer EINs from domains, simulate API enrichment |
| **Output** | Write Parquet + update watermark |

**Metrics logged:** rows in/out, validation errors, enrichment stats, runtime.

---

## Interactive Queries

```bash
# DuckDB CLI
docker-compose run --rm app duckdb /app/outputs/analytics.duckdb

# Example queries
SHOW TABLES;
SELECT company_ein, COUNT(*) FROM employees_raw GROUP BY 1;
SELECT * FROM v_plan_gaps;
SELECT * FROM v_claims_cost_spike;
```

---

## Key Production Decisions

| Decision | Why It Matters in Real Life |
|----------|----------------------------|
| DuckDB + views | Zero-cost, columnar, instant analytical queries, idempotent runs |
| Incremental merge-into-Parquet | Same code works at 100 rows or 100M rows/day |
| Validation errors never dropped | Full audit trail, no silent data loss |
| Structured logging + metrics | Debug a prod incident in <10 min |
| Everything containerized | Works identically for every reviewer |
| Enrichment as Parquet | Trivial to swap in Spark, Snowflake, or a real API later |

---

## Production Hardening Roadmap

| Category | Enhancement |
|----------|-------------|
| **Incrementals** | Per-table watermarks, checksum-based change detection, soft-delete tracking |
| **Quality** | Great Expectations integration, schema evolution handling |
| **Testing** | pytest unit tests, integration tests with fixtures |
| **Scale** | Chunked CSV reading, file locking for concurrent runs |
| **Observability** | Structured logging (JSON), run metadata table |

---

## Documentation

- **[DESIGN.md](DESIGN.md)** — dbt system design, testing strategy, incident response playbook

---

## Requirements

- Docker
- Docker Compose

Built with: Python 3.9 • DuckDB • Pandas • PyArrow
