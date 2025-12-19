
## Business Logic & Funnel Definitions
- **Funnel Entry Definition**: A deal "enters" a funnel step at the *earliest* timestamp where an event (stage change or activity) maps to that step.
- **Mixed Event Sources**: 
    - **Stage Changes**: The primary driver. Mapping is maintained in `stg_stages`. 
    - **Activities**: Used for granular steps (e.g., "Sales Call 1", "Sales Call 2"). We parse `activity_type_name` to infer these steps. 
    - **Conflict Resolution**: If a deal has both a stage change and an activity mapping to the same step, the *earlier* event wins. If timestamps are identical, stage changes get priority.
- **Re-entries**: The current model logic (`int_pipedrive_deal_funnel_events`) counts only the *first* entry per deal per step. Subsequent re-entries (e.g., moving back and forth) are ignored for this specific "Funnel Conversion" view.
- **Timezone Standardization**: All timestamps (`change_time`, `due_to`) are converted to UTC explicitly before logic application to ensure consistency across global teams.

## Data Quality & Integrity
- **Missing Timestamps**: Events with `NULL` timestamps are effectively useless for funnel analysis and are excluded. We rely on (`not_null`) tests to flag if this becomes a systemic issue.
- **Duplicate Events**: The source data (`deal_changes`) is an immutable log. We assume valid distinct events based on `(deal_id, unique_event_id)`. We prioritize robustness by using `row_number()` to deduplicate effectively at the grain of `(deal_id, funnel_step)`.
- **Orphaned Data**: Activities or changes without a valid `deal_id` are excluded via inner joins or `not_null` filters.

## Architecture & Scaling Considerations
- **Materialization Strategy**: 
    - `staging` models are standard Views (or Tables in this assessment for simplicity) to provide a clean interface to raw data.
    - `intermediate` and `marts` are materialized as Tables to prioritize query performance for BI tools.
    - **Future Improvement**: For high-volume production, `int_pipedrive_deal_funnel_events` is a prime candidate for an *Incremental Model* strategy, partitioning by `event_ts_utc`.
- **Hardcoded Mappings**: Activity string matching (`LIKE '%Sales Call 1%'`) is brittle. In a production environment, we would recommend a cleaner upstream data contract (e.g., explicit `funnel_step_id` or a dedicated reference table) to avoid "magic strings" in SQL.
