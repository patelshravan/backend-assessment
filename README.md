# Project Root - Customer Data Pipeline

This project runs a simple data pipeline with 3 Docker services:

- `mock-server` (Flask): serves customer data from `mock-server/data/customers.json` on port `5000`
- `pipeline-service` (FastAPI): ingests from the Flask API into PostgreSQL on port `8000`
- `postgres`: stores ingested customers

Flow:

`Flask (JSON) -> FastAPI (/api/ingest) -> PostgreSQL (customers table) -> FastAPI query endpoints`

## Prerequisites

- Docker Desktop (running)
- Docker + docker-compose

## Start all services

From the repo root:

```powershell
docker-compose up --build -d
```

## Test Flask endpoints

Health check:

```powershell
curl http://localhost:5000/api/health
```

Paginated list:

```powershell
curl "http://localhost:5000/api/customers?page=1&limit=5"
```

Single customer:

```powershell
curl "http://localhost:5000/api/customers/CUST-0001"
```

## Ingest Flask data into PostgreSQL (dlt)

```powershell
curl -X POST http://localhost:8000/api/ingest
```

## Test FastAPI endpoints

Paginated list from DB:

```powershell
curl "http://localhost:8000/api/customers?page=1&limit=5"
```

Single customer from DB:

```powershell
curl "http://localhost:8000/api/customers/CUST-0001"
```

## Notes

- Re-running `/api/ingest` will upsert/merge by `customer_id`.
- FastAPI returns `404` when a customer id is missing.

"# backend-assessment" 
