{{ config(materialized='table') }}

select
  {{ sk(['customer_bk','valid_from']) }} as customer_sk,
  customer_bk,
  first_name,
  last_name,
  email,
  valid_from as effective_from,
  valid_to   as effective_to,
  case when valid_to is null then true else false end as is_current
from {{ ref('snap_customers') }}
