with src as (
  select
    customer_id                                as customer_bk,
    trim(first_name)                           as first_name,
    trim(last_name)                            as last_name,
    lower(email)                               as email,    
    try_to_timestamp_ntz(convert_timezone('UTC', to_timestamp_tz(created_at))) as created_ts
  from {{ source('ninja_lending','customers') }}
)
select
  {{ sk(['customer_bk']) }}                    as customer_sk,
  customer_bk, first_name, last_name, email, created_ts
from src
