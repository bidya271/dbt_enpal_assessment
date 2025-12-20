# Enpal Senior BI Assessment - Pipedrive Analysis

## Overview
This project transforms raw Pipedrive CRM data into a clean, actionable Sales Funnel report using **dbt**, **Postgres**, and **Docker**. 

The goal is to provide a robust analytics foundation that tracks deals as they move through funnel steps (Lead Generation -> Closing), handling complexities like mixed event sources (Stage Changes vs. Activities) and timezone normalization.

## Architecture & Design Logic
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

## Tech Stack & Setup
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

## Quality Assurance
- **Testing**: 
    - Added `not_null` and `unique` constraints on all primary keys.
    - Custom Singular Test (`monthly_spike_check`) monitors data stability (alerts if month-over-month growth exceeds 500%).
- **Documentation**: Assumes `dbt docs generate` will be part of the CI/CD pipeline.

## Scaling Thoughts
For a production environment handling millions of rows:
- **Incremental Models**: The intermediate layer should leverage `incremental` strategies (e.g., processing only `event_ts > max(this)`).
- **Partitioning**: Postgres tables should be partitioned by Month for faster report generation.

---

## Assessment Review Notes

### 1. Design & Architecture Decisions
- **Layered Approach**: Strictly adhered to `Staging` (cleaning) -> `Intermediate` (logic/logic) -> `Marts` (presentation). This separation allows for easier debugging and reusability.
- **Materialization Strategy**: 
  - Switched `staging` to **Views** to ensure freshness and reduce storage, assuming they are light transformations.
  - Kept `intermediate` and `marts` as **Tables** to ensure downstream BI performance. In a high-volume production scenario, I would upgrade `int_pipedrive_deal_funnel_events` to an **Incremental** model.
- **Timezone**: Explicitly standardized on UTC (`at time zone 'utc'`) early in the staging layer to avoid timezone bugs in aggregation.

### 2. Key Challenges & Solutions
- **Event Deduplication**: Pipedrive data can be noisy. I implemented a robust `row_number()` window function in `int_pipedrive_deal_funnel_events` to deterministically pick the *first* entry per funnel step.
- **Mixed Sources (Stage vs Activity)**: Unified these distinct streams into a common "Event" schema before aggregation. This makes adding a 3rd source (e.g., "Emails") trivial in the future without rewriting the core logic.
- **Data Gaps**: Used `generate_series` in the final mart to ensure months with ZERO deals still show up in reports, preventing misleading line charts in dashboards.

### 3. Data Quality (The "Senior" Check)
- **Tests**: Added `unique` and `not_null` tests on primary keys.
- **Singular Test**: Implemented `monthly_spike_check` to mathematically detect anomalies (growth > 500%). This is proactive data engineering compared to reactive bug fixing.
- **Assumptions**: Documented explicitly in `ASSUMPTIONS.md`. A senior engineer doesn't just write code; they define the contract.

### 4. Future Improvements (Talking Points)
- **CI/CD**: I would add `sqlfluff` for linting and run `dbt test` on every PR.
- **Orchestration**: For production, I'd wrap this in Airflow or Dagster.
- **Performance**: Partitioning the `deal_changes` source table by time would significantly speed up the daily incremental loads.
