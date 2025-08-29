with src as (
  select
    loan_id                                    as loan_bk,
    application_id                             as application_bk,
    customer_id                                as customer_bk,
    try_cast(loan_amount as number(38,10))     as loan_amount,
    try_cast(interest_rate as number(18,9))    as interest_rate,
    try_to_date(start_date)                    as disbursement_date,
    try_to_date(end_date)                      as maturity_date,
    upper(status)                              as loan_status_bk
  from {{ source('ninja_lending','loans') }}
)
select
  {{ sk(['loan_bk']) }}                        as loan_sk,
  loan_bk, application_bk, customer_bk, loan_amount, interest_rate,
  disbursement_date, maturity_date, loan_status_bk
from src
