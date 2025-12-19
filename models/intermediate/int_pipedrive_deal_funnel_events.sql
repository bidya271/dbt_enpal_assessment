-- models/intermediate/int_pipedrive_deal_funnel_events.sql
-- Purpose: For each deal and business funnel step, capture the first timestamp the deal entered that step.
-- Grain: deal_id, funnel_step, step_entered_at

with stage_changes as (

    -- take only stage change events and cast timestamp
    select
        deal_id,
        change_time::timestamp as change_time,
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
