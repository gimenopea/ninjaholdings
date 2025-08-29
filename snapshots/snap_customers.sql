{% snapshot snap_customers %}
{{
  config(
    target_schema='SNAPSHOTS',
    unique_key='customer_bk',
    strategy='timestamp',
    updated_at='created_ts'
  )
}}
select * from {{ ref('stg_customers') }}
{% endsnapshot %}
