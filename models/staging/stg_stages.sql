-- models/staging/stg_stages.sql
-- Purpose: Clean and standardize Pipedrive stages, and map them to business funnel steps.

with source as (

    select
        stage_id,
        stage_name
    from {{ source('postgres_public', 'stages') }}

),

normalized as (

    select
        stage_id,
        stage_name,

        -- Normalize funnel step names to match the assignment exactly
        case
            when lower(stage_name) like 'lead generation%' then 'Lead Generation'
            when lower(stage_name) like 'qualified lead%' then 'Qualified Lead'
            when lower(stage_name) like 'needs assessment%' then 'Needs Assessment'
            when lower(stage_name) like 'proposal/quote preparation%' then 'Proposal/Quote Preparation'
            when lower(stage_name) like 'negotiation%' then 'Negotiation'
            when lower(stage_name) like 'closing%' then 'Closing'
            when lower(stage_name) like 'implementation/onboarding%' then 'Implementation/Onboarding'
            when lower(stage_name) like 'follow-up/customer success%' then 'Follow-up/Customer Success'
            when lower(stage_name) like 'renewal/expansion%' then 'Renewal/Expansion'
            else stage_name
        end as funnel_step
    from source

)

select *
from normalized
