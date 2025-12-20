-- models/staging/stg_activity.sql
-- Purpose: Clean activities and enrich them with activity type metadata.

with source as (

    select
        activity_id,
        type,
        assigned_to_user,
        deal_id,
        done,
        due_to
    from {{ source('postgres_public', 'activity') }}

),

typed as (

    select
        activity_id,
        deal_id,
        assigned_to_user as user_id,
        done,
        due_to::timestamp as due_to,
        type as activity_type_code
    from (
        select 
            *,
            row_number() over (partition by activity_id order by due_to desc) as rn
        from source
    ) s
    where rn = 1

),

joined as (

    select
        a.activity_id,
        a.deal_id,
        a.user_id,
        a.done,
        a.due_to,
        a.activity_type_code,
        at.activity_type_id,
        at.activity_type_name,
        at.activity_type_code as activity_type_code_normalized
    from typed a
    left join {{ ref('stg_activity_types') }} at
        on a.activity_type_code = at.activity_type_code

)

select *
from joined
