# SA Regional Data Platform

An end-to-end local data platform analysing **South Australian road crashes against regional population**, built to demonstrate production-style data engineering: ELT ingestion from two public APIs, a medallion architecture (bronze → silver → gold) in dbt, a conformed-dimension star schema, Airflow orchestration, data quality testing, and CI.

> Full documentation, architecture diagrams and a "what this demonstrates" guide land in Phase 7 — this README grows with the build.

## Data sources (both free, keyless, CC-BY)

| Source | Dataset | Grain |
|---|---|---|
| [Data.SA](https://data.sa.gov.au/data/dataset/road-crash-data) (CKAN API) | SA Road Crash Data, latest 5-year extract (crash / casualty / unit tables) | One row per crash, per person injured, per vehicle involved |
| [ABS Data API](https://data.api.abs.gov.au) (SDMX) | Estimated Resident Population by LGA (`ABS_ANNUAL_ERP_LGA2024`) | LGA × year × sex × age band |

## Quickstart (so far)

```bash
cp .env.example .env
docker compose up -d warehouse-db          # Postgres warehouse on localhost:5433
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m ingestion.load_raw --source all
```

This lands five raw (bronze) tables in the `raw` schema: `data_sa_crashes`, `data_sa_casualties`, `data_sa_units`, `abs_erp_lga`, `abs_lga_codelist`.
