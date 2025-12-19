-- models/staging/stg_deal_changes.sql
-- Purpose: Clean and type the raw deal_changes event log from Pipedrive.

with source as (

    select
        deal_id,
        change_time,
        changed_field_key,
        new_value
    from {{ source('postgres_public', 'deal_changes') }}

),

typed as (

    select
        deal_id,
        change_time::timestamp as change_time,
        changed_field_key,
        new_value,

        -- Convenience typed columns for the two key fields we care about
        case
            when changed_field_key = 'stage_id'
                then new_value::integer
        end as new_stage_id,

        case
            when changed_field_key = 'user_id'
                then new_value::integer
        end as new_user_id
    from source

)

select *
from typed;
