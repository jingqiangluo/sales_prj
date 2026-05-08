# Sales Data Pipeline

End-to-end data engineering project that processes messy real-world sales data using **Python, dbt, PostgreSQL, and Docker**.

## Overview

This pipeline ingests a dirty CSV file (`example_sales_data.csv`) containing inconsistent dates, store IDs, prices, and missing values. It cleans the data, loads it into PostgreSQL, and builds analytical models using dbt.

**Goal**: Demonstrate core data engineering skills including ETL, data modeling, testing, and containerization.

## Approach and Architecture

This project implements a complete end-to-end data pipeline that takes messy, real-world sales data from a CSV file and turns it into clean, reliable analytical models.

### Overall Design Philosophy

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold). This layered approach ensures that raw data is gradually cleaned, enriched, and transformed into high-quality, consumption-ready datasets.

### Architecture Flow

1. **Source Layer**  
   The raw `example_sales_data.csv` file, which contains many data quality issues such as inconsistent date formats, messy store names, missing values, and irregular formatting.

2. **Raw Layer**  
   Python ETL script reads the CSV, performs initial validation, and loads the data into the `raw.sales_raw` table in PostgreSQL. This layer preserves the original data as a single source of truth.

3. **Staging Layer** (Silver)  
   Using dbt, the data is cleaned and standardized. This includes parsing multiple date formats, normalizing store IDs and regions, handling nulls, calculating `total_amount`, and adding derived columns.

4. **Intermediate Layer**  
   Aggregates the cleaned data into daily metrics by store and region. This layer applies business logic such as counting transactions, calculating discount rates, and identifying top payment methods.

5. **Mart Layer** (Gold)  
   Produces the final analytical model (`mart_sales_daily`) containing ready-to-use KPIs like daily revenue, transaction count, and average order value. Optimized for reporting and analysis.

### Key Design Decisions

- **Separation of Responsibilities**: Python handles extraction and heavy data cleaning (strong at handling messy CSV), while dbt manages modeling, testing, and documentation.
- **Incremental Loading**: Implemented in the staging model for efficiency on subsequent runs.
- **Data Quality First**: Cleaning happens early in the staging layer, with validation at every step.
- **Reproducibility**: Everything runs in Docker containers using Docker Compose.
- **Maintainability**: Clear separation of layers and use of dbt best practices.

### Technology Choices

Python + pandas was chosen for the ETL step due to its excellent capabilities for data cleaning. dbt was selected for transformations because it provides version control, testing, documentation, and incremental processing. PostgreSQL serves as a lightweight but realistic local substitute for cloud data warehouses like Snowflake.

This architecture is production-oriented, scalable, and follows modern data engineering best practices while remaining simple enough to run locally.


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
