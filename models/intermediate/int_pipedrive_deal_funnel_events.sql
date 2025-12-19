/*
  models/intermediate/int_pipedrive_deal_funnel_events.sql

  Incremental canonical first-entry events per deal + funnel_step.

  Strategy:
  - Build a deterministic "deal_step_fingerprint" for each candidate event
    (deal_id + funnel_step + event_ts OR stable change/activity id).
  - When dbt is run incremental, upsert new/changed rows using ON CONFLICT.
  - Provide a small defensive pipeline allowing fallback labels (raw_stage_<id>)
    so we don't silently drop events. The mart can choose to filter raw labels.
*/

{{ config(
    materialized='incremental',
    unique_key='deal_step_fingerprint'
) }}

with
-- Stage change candidate events: normalized timestamp, typed ids
stage_changes as (
  select
    deal_id,
    change_time::timestamptz at time zone 'utc' as event_ts_utc,
    new_stage_id,
    -- deterministic stable id for a stage change: if the source has an 'id' column use it,
    -- otherwise build a fingerprint from deal_id+change_time+new_value
    coalesce(
      -- if change_id exists in staging (string), use it
      (case when pg_typeof(new_stage_id) is not null then null end),
      md5(concat(coalesce(deal_id::text,''),'|',coalesce(change_time::text,''),'|',coalesce(new_stage_id::text,'')))
    ) as change_fingerprint
  from {{ ref('stg_deal_changes') }}
  where changed_field_key = 'stage_id'
    and new_stage_id is not null
),

-- map stage_id -> canonical funnel_step (fall back to raw label if missing)
stage_events as (
  select
    sc.deal_id,
    sc.new_stage_id as stage_id,
    coalesce(s.funnel_step, 'raw_stage_' || sc.new_stage_id::text) as funnel_step,
    sc.event_ts_utc,
    sc.change_fingerprint as source_id,
    'stage' as event_source
  from stage_changes sc
  left join {{ ref('stg_stages') }} s
    on sc.new_stage_id = s.stage_id
),

-- activity-based funnel steps (Sales Call 1 / 2, etc.)
activity_call_events as (
  select
    a.deal_id,
    null::int as stage_id,
    case
      when lower(a.activity_type_name) like '%sales call 1%' then 'Sales Call 1'
      when lower(a.activity_type_name) like '%sales call 2%' then 'Sales Call 2'
      else null
    end as funnel_step,
    (a.due_to::timestamptz at time zone 'utc') as event_ts_utc,
    md5(concat(coalesce(a.activity_id::text,''),'|',coalesce(a.due_to::text,''))) as source_id,
    'activity' as event_source
  from {{ ref('stg_activity') }} a
  where a.activity_type_name is not null
    and (
      lower(a.activity_type_name) like '%sales call 1%'
      or lower(a.activity_type_name) like '%sales call 2%'
    )
),

-- union candidate events
union_all_events as (
  select deal_id, funnel_step, event_ts_utc, source_id, event_source from stage_events
  union all
  select deal_id, funnel_step, event_ts_utc, source_id, event_source from activity_call_events
),

-- filter and rank: deterministic first-entry per deal + funnel_step
ranked as (
  select
    u.deal_id,
    u.funnel_step,
    u.event_ts_utc,
    u.event_source,
    u.source_id,
    row_number() over (
      partition by u.deal_id, u.funnel_step
      order by u.event_ts_utc asc,
               case when u.event_source = 'stage' then 0 else 1 end asc,
               u.source_id asc
    ) as rn
  from union_all_events u
  where u.event_ts_utc is not null
    and u.funnel_step is not null
)

, first_events as (
  select
    deal_id,
    funnel_step,
    event_ts_utc as step_entered_at,
    -- deterministic fingerprint for this deal+step+timestamp
    md5(concat(coalesce(deal_id::text,''),'|',coalesce(funnel_step,''),'|',coalesce(event_ts_utc::text,''))) as deal_step_fingerprint,
    min(source_id) as first_source_id,
    min(event_source) as first_event_source
  from ranked
  where rn = 1
  group by 1,2,3
)

-- Final upsertable select
select
  deal_id,
  funnel_step,
  step_entered_at,
  deal_step_fingerprint,
  first_source_id,
  first_event_source
from first_events
