{% snapshot deal_changes_snapshot %}
{{
  config(
    target_schema='snapshots',
    unique_key='deal_change_fingerprint',
    strategy='check',
    check_cols=['change_time','new_value','changed_field_key']
  )
}}

select
  -- deterministic fingerprint for each raw change row to use as unique_key
  md5(concat(coalesce(deal_id::text,''),'|',coalesce(change_time::text,''),'|',coalesce(new_value::text,''))) as deal_change_fingerprint,
  *
from {{ source('postgres_public','deal_changes') }}

{% endsnapshot %}
