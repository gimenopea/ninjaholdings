{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='payment_bk',
  on_schema_change='append_new_columns'
) }}

with base as (
  select
    payment_bk, loan_bk, customer_bk, payment_date, payment_amount, payment_type_bk
  from {{ ref('stg_payments') }}
  {% if is_incremental() %}
    where payment_date >= dateadd(day, -14, current_date)
  {% endif %}
)
select
  {{ sk(['payment_bk']) }}     as payment_sk,
  b.payment_bk,
  l.loan_sk,
  c.customer_sk,
  d.date_sk                    as payment_date_sk,
  t.payment_type_sk,
  b.payment_amount
from base b
left join {{ ref('fct_loans') }} l     on l.loan_bk = b.loan_bk
left join {{ ref('dim_customer') }} c  on c.customer_bk = b.customer_bk and c.is_current
left join {{ ref('dim_date') }} d      on d.calendar_date = b.payment_date
left join {{ ref('dim_payment_type') }} t on t.payment_type_bk = b.payment_type_bk
