# Enpal Senior BI Assessment - Pipedrive Analysis

## Overview
This project transforms raw Pipedrive CRM data into a clean, actionable Sales Funnel report using **dbt**, **Postgres**, and **Docker**. 

The goal is to provide a robust analytics foundation that tracks deals as they move through funnel steps (Lead Generation -> Closing), handling complexities like mixed event sources (Stage Changes vs. Activities) and timezone normalization.

## 🏗 Architecture & Design Logic
The project follows a modular "Modern Data Stack" layer approach:

1.  **Staging (`models/staging`)**:
    *   1:1 mapping with source tables.
    *   Type casting (e.g., timestamps to UTC) and column renaming for consistency.
    *   *Decision*: Light transformations only; keep close to raw for debuggability.

2.  **Intermediate (`models/intermediate`)**:
    *   `int_pipedrive_deal_funnel_events`: The core logic engine. 
    *   Combines `stage_changes` and `activities` into a single "Funnel Event" stream.
    *   Handles deduplication (finding the *first* time a deal hit a step).
    *   *Decision*: Materialized as a Table to improve downstream performance, acting as a reliable "Fact Table" for funnel analysis.

3.  **Marts (`models/marts`)**:
    *   `rep_sales_funnel_monthly`: Aggregates the intermediate events into a monthly report.
    *   Fills in missing months (using `generate_series`) to ensure continuous reporting even for low-volume periods.

## 🛠 Tech Stack & Setup
- **dbt Core**: Transformation & Testing.
- **Postgres 15**: Data Warehouse (running in Docker).
- **Docker Compose**: Container orchestration.

### Quick Start
1.  **Prerequisites**: Docker Desktop installed.
2.  **Launch Database**: 
    ```bash
    docker compose up -d
    ```
    *Note: Exposes Postgres on port `15432` (mapped to container 5432).*
3.  **Install Dependencies**:
    ```bash
    # (Assuming python venv is active)
    pip install dbt-postgres
    dbt deps
    ```
4.  **Run Pipeline**:
    ```bash
    dbt build
    ```
    *(Runs seeds, models, and tests in order)*

## ✅ Quality Assurance
- **Testing**: 
    - Added `not_null` and `unique` constraints on all primary keys.
    - Custom Singular Test (`monthly_spike_check`) monitors data stability (alerts if month-over-month growth exceeds 500%).
- **Documentation**: Assumes `dbt docs generate` will be part of the CI/CD pipeline.

## 🚀 Scaling Thoughts
For a production environment handling millions of rows:
- **Incremental Models**: The intermediate layer should leverage `incremental` strategies (e.g., processing only `event_ts > max(this)`).
- **Partitioning**: Postgres tables should be partitioned by Month for faster report generation.

