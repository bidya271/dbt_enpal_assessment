-- models/intermediate/int_pipedrive_deal_funnel_events.sql
-- Purpose: For each deal and business funnel step, capture the first timestamp the deal entered that step.
-- Grain: deal_id, funnel_step, step_entered_at

with stage_changes as (

    -- take only stage change events and cast timestamp
    select
        deal_id,
        change_time::timestamp as change_t-- models/intermediate/int_pipedrive_deal_funnel_events.sql
-- Purpose: For each deal and canonical funnel_step, select the canonical first-entry event (deterministic).
-- Grain: deal_id, funnel_step, step_entered_at (timestamp with timezone normalized to UTC)

with
-- 1. stage change events: normalized timestamp, only stage changes
stage_changes as (
  select
    deal_id,
    change_time::timestamptz at time zone 'utc' as event_ts_utc,
    new_stage_id,
    change_id  -- stable unique id per change row (if available)
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
--    Normalize timestamp and capture stable activity id for tie-breaking
activity_call_events as (
  select
    a.deal_id,
    null::int as stage_id,
    -- canonicalize activity->funnel mapping by joining to stg_activity_types if available
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
  select deal_id, funnel_step, event_ts_utc, change_id as source_id, event_source
  from stage_events
  union all
  select deal_id, funnel_step, event_ts_utc, activity_id::text as source_id, event_source
  from activity_call_events
),

-- 5. deterministic canonicalization: pick the first event per deal + funnel_step
--    Order: earliest timestamp; if tie, prefer stage events over activity events; if still tie, use source_id (stable id) asc
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
               -- prefer stage events in ties (stage < activity)
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
order by deal_id, step_entered_at;
ime,
        new_stage_id
    from {{ ref('stg_deal_changes') }}
    where changed_field_key = 'stage_id'
      and new_stage_id is not null

),

stage_first_entries as (

    -- for each deal & stage, get the first time it entered that stage
    select
        deal_id,
        new_stage_id as stage_id,
        min(change_time) as step_entered_at
    from stage_changes
    group by deal_id, new_stage_id

),

-- map stage_id to canonical funnel_step name
stage_with_funnel as (

    select
        sfe.deal_id,
        sfe.stage_id,
        sfe.step_entered_at,
        st.funnel_step
    from stage_first_entries sfe
    left join {{ ref('stg_stages') }} st
      on sfe.stage_id = st.stage_id
    where st.funnel_step is not null

),

-- Identify Sales Call events (Sales Call 1 / Sales Call 2) from activities
call_events as (

    select
        a.deal_id,
        case
            when lower(a.activity_type_name) like '%sales call 1%' then 'Sales Call 1'
            when lower(a.activity_type_name) like '%sales call 2%' then 'Sales Call 2'
            else null
        end as funnel_step,
        a.due_to as step_entered_at
    from {{ ref('stg_activity') }} a
    where a.activity_type_name is not null
      and (lower(a.activity_type_name) like '%sales call 1%' or lower(a.activity_type_name) like '%sales call 2%')
),

-- union stage-based steps and call-based steps
union_all_events as (

    select
        deal_id,
        funnel_step,
        step_entered_at
    from stage_with_funnel

    union all

    select
        deal_id,
        funnel_step,
        step_entered_at
    from call_events

),

-- Deduplicate: keep first entry per deal & funnel_step
first_event_per_step as (

    select
        deal_id,
        funnel_step,
        min(step_entered_at) as step_entered_at
    from union_all_events
    where funnel_step is not null
    group by deal_id, funnel_step

)

select
    deal_id,
    funnel_step,
    step_entered_at
from first_event_per_step
order by deal_id, step_entered_at;
