{{ config(materialized='table') }}

select
  {{ sk(['application_bk']) }}  as application_sk,
  application_bk,
  c.customer_sk,
  application_date,
  loan_amount_requested,
  status_bk
from {{ ref('stg_loan_applications') }} a
left join {{ ref('dim_customer') }} c
  on c.customer_bk = a.customer_bk and c.is_current
