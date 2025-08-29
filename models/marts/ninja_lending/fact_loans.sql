{{ config(
  materialized='incremental',
  incremental_strategy='merge',
  unique_key='loan_bk',
  on_schema_change='append_new_columns'
) }}

with base as (
  select
    loan_bk, application_bk, customer_bk,
    loan_amount, interest_rate, disbursement_date, maturity_date, loan_status_bk
  from {{ ref('stg_loans') }}
  {% if is_incremental() %}
    where disbursement_date >= dateadd(day, -7, current_date)
       or maturity_date     >= dateadd(day, -7, current_date)
  {% endif %}
)
select
  {{ sk(['loan_bk']) }}        as loan_sk,
  b.loan_bk,
  a.application_sk,
  c.customer_sk,
  d1.date_sk                   as disbursement_date_sk,
  d2.date_sk                   as maturity_date_sk,
  s.loan_status_sk             as current_status_sk,
  b.loan_amount,
  b.interest_rate
from base b
left join {{ ref('dim_loan_application') }} a on a.application_bk = b.application_bk
left join {{ ref('dim_customer') }} c on c.customer_bk = b.customer_bk and c.is_current
left join {{ ref('dim_date') }} d1 on d1.calendar_date = b.disbursement_date
left join {{ ref('dim_date') }} d2 on d2.calendar_date = b.maturity_date
left join {{ ref('dim_loan_status') }} s on s.loan_status_bk = b.loan_status_bk
