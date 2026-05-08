# Sales Data Pipeline

End-to-end data engineering project that processes messy real-world sales data using **Python, dbt, PostgreSQL, and Docker**.

## Overview

This pipeline ingests a dirty CSV file (`example_sales_data.csv`) containing inconsistent dates, store IDs, prices, and missing values. It cleans the data, loads it into PostgreSQL, and builds analytical models using dbt.

**Goal**: Demonstrate core data engineering skills including ETL, data modeling, testing, and containerization.

## Architecture
``` mermaid
graph TD
    A[example_sales_data.csv]-->B[Python ETL];
    B-->C[raw.sales_raw PostgreSQL];
    C-->D[dbt Staging Layer];
    D-->E[dbt Intermediate Layer];
    E-->F[dbt Mart Layer];
```

## Medallion Architecture

This project follows the **Medallion Architecture** (also known as Bronze-Silver-Gold), a widely used design pattern in modern data engineering.

| Layer           | Schema       | Purpose                                                                 | Materialization     | Key Transformations |
|-----------------|--------------|-------------------------------------------------------------------------|---------------------|---------------------|
| **Raw**         | `raw`        | Landing zone for raw, unprocessed data from source                      | Table               | None (raw load) |
| **Staging**     | `analytics`  | Data cleaning, standardization, validation, and basic enrichment        | Incremental View    | TRIM, INITCAP, COALESCE, date parsing, calculated fields |
| **Intermediate**| `analytics`  | Business logic, aggregations, and reusable intermediate datasets       | Table               | Daily metrics, grouping, derived KPIs |
| **Mart**        | `analytics`  | Final, consumption-ready models for analytics, reporting, and BI tools  | Table               | Aggregated KPIs, easy-to-consume views |

### Layer Responsibilities

- **Raw Layer**: Single source of truth for original data
- **Staging Layer**: Makes data clean, consistent, and queryable
- **Intermediate Layer**: Applies business rules and creates reusable building blocks
- **Mart Layer**: Optimized for end-user consumption (dashboards, reports, analysts)

This layered approach improves **data quality**, **maintainability**, and **performance**.

## Tech Stack

| Component          | Technology                    | Purpose |
|--------------------|-------------------------------|--------|
| **Orchestration**  | ![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white) + Docker Compose | Pipeline orchestration |
| **ETL**            | pandas + SQLAlchemy           | Data ingestion & cleaning |
| **Database**       | PostgreSQL                    | Data storage |
| **Transformation** | dbt (dbt-postgres)            | Analytics modeling |
| **Testing**        | pytest + dbt tests            | Data quality & validation |
| **Containerization**| Docker                        | Reproducible environment |

## Project Structure

```bash
sales_prj/
├── docker-compose.yml                 # Main container orchestration
├── run_pipeline.py                    # End-to-end pipeline runner
├── example_sales_data.csv             # Raw messy data source
│
├── etl/                               # Python ETL Layer
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── load_sales.py                  # Data cleaning & loading logic
│   └── test_etl.py                    # Python unit tests
│
└── dbt/                               # dbt Transformation Layer
    ├── Dockerfile
    ├── dbt_project.yml
    ├── profiles.yml
    ├── packages.yml
    ├── models/
    │   ├── staging/
    │   │   └── stg_sales.sql          # Cleaning & standardization
    │   ├── intermediate/
    │   │   └── int_sales_daily.sql    # Daily aggregations
    │   └── marts/
    │       └── mart_sales_daily.sql   # Final analytical models
    └── schema.yml                     # Data quality tests
```

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Docker Compose
- Git (optional)

### 1. Run the Full Pipeline (Recommended)

```bash
# Run the entire pipeline in one command
python run_pipeline.py
```

### 2. Manual Step-by-Step Execution

```bash
# 1. Start the PostgreSQL database
docker compose up -d postgres

# 2. Run the Python ETL process (cleaning + loading)
docker compose run --rm etl

# 3. Run dbt transformations (Staging → Intermediate → Mart)
docker compose exec -T dbt dbt run --project-dir /usr/app --profiles-dir /usr/app

# 4. Run data quality tests
docker compose exec -T dbt dbt test --project-dir /usr/app --profiles-dir /usr/app
```

## Useful Commands

### Docker & Pipeline

```bash
# Start all services
docker compose up -d

# Run the full pipeline
python run_pipeline.py

# Rebuild and run only ETL
docker compose build etl --no-cache && docker compose run --rm etl

# Run only dbt models
docker compose exec -T dbt dbt run --project-dir /usr/app --profiles-dir /usr/app

# Run only tests
docker compose exec -T dbt dbt test --project-dir /usr/app --profiles-dir /usr/app
```