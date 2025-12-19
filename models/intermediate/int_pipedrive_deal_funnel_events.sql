-- models/intermediate/int_pipedrive_deal_funnel_events.sql
-- Purpose: canonical first-entry events per deal and funnel_step (deterministic, UTC-normalized).
-- Grain: deal_id, funnel_step, step_entered_at (timestamp with timezone normalized to UTC)

with
-- 1. stage change events: normalized timestamp, only stage changes
stage_changes as (
  select
    deal_id,
    change_time::timestamptz at time zone 'utc' as event_ts_utc,
    new_stage_id,
    change_id
  from {{ ref('stg_deal_changes') }}
  where changed_field_key = 'stage_id'
    and new_stage_id is not null
),

-- 2. map stage_id -> canonical funnel_step (only keep mapped stages)
stage_first_entries as (
  select
    sc.deal_id,
    sc.new_stage_id as stage_id,
    sc.event_ts_utc,
    sc.change_id
  from stage_changes sc
  left join {{ ref('stg_stages') }} s
    on sc.new_stage_id = s.stage_id
  where s.funnel_step is not null
),

stage_events as (
  select
    sfe.deal_id,
    sfe.stage_id,
    s.funnel_step,
    sfe.event_ts_utc,
    sfe.change_id,
    'stage' as event_source
  from stage_first_entries sfe
  join {{ ref('stg_stages') }} s
    on sfe.stage_id = s.stage_id
),

-- 3. activity-based funnel steps (Sales Call 1 / 2, etc.)
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
    a.activity_id as activity_id,
    'activity' as event_source
  from {{ ref('stg_activity') }} a
  where a.activity_type_name is not null
    and (
      lower(a.activity_type_name) like '%sales call 1%'
      or lower(a.activity_type_name) like '%sales call 2%'
    )
),

-- 4. union of candidate events (stage + activity)
union_all_events as (
  select deal_id, funnel_step, event_ts_utc, change_id::text as source_id, event_source
  from stage_events
  union all
  select deal_id, funnel_step, event_ts_utc, activity_id::text as source_id, event_source
  from activity_call_events
),

-- 5. deterministic canonicalization: pick the first event per deal + funnel_step
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
  where u.funnel_step is not null
    and u.event_ts_utc is not null
)

select
  deal_id,
  funnel_step,
  event_ts_utc as step_entered_at
from ranked
where rn = 1
order by deal_id, step_entered_at
