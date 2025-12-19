
## Canonical definitions
- "Entered a funnel step": first canonical event where either a stage change maps to the funnel step OR an activity maps to the funnel step. We select the earliest timestamp, normalized to UTC.
- Tie-breaker: if multiple events have identical timestamps, prefer stage-based events, then use the stable row id (change_id or activity_id) to break ties deterministically.
- Re-entries: we count only the first time a deal entered a funnel step (historical re-entries not counted). For re-entry analysis, a separate metric/model should be created.

## Timestamp handling
- All timestamps are normalized to UTC (`AT TIME ZONE 'UTC'`) before deduplication and aggregation.
- When event timestamp is null, the event is excluded (we prefer explicit exclusion and a test to surface missing timestamps).

## Stage/Activity mappings
- Stage -> funnel_step mapping is maintained in `stg_stages`. Edit `stg_stages` to change mappings rather than changing SQL logic.
- Activity -> funnel_step mappings are currently detected in `int_pipedrive_deal_funnel_events` by searching `activity_type_name`. For production, centralize this mapping in `stg_activity_types`.

## Production notes
- For large production datasets, consider:
  - Adding partial indexes on `(deal_id, event_ts)` in the source table to speed first-entry retrievals.
  - Materializing `int_pipedrive_deal_funnel_events` as an incremental model with a stable `unique_key` if the dataset grows.
