-- models/staging/stg_activity_types.sql
-- Purpose: Clean the activity types lookup (e.g. Sales Call 1, Sales Call 2)

with source as (

    select
        id,
        name,
        active,
        type
    from {{ source('postgres_public', 'activity_types') }}

)

select
    id as activity_type_id,
    name as activity_type_name,
    active,
    type as activity_type_code
from source;
