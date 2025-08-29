with src as (
  select
    payment_id                                 as payment_bk,
    loan_id                                    as loan_bk,
    customer_id                                as customer_bk,
    /* handle string/NTZ/TZ → UTC date */
    try_to_date(convert_timezone('UTC', to_timestamp_tz(payment_date))) as payment_date,
    try_cast(payment_amount as number(38,10))  as payment_amount,
    upper(payment_type)                        as payment_type_bk
  from {{ source('ninja_lending','payments') }}
)
select
  {{ sk(['payment_bk']) }}                     as payment_sk,
  payment_bk, loan_bk, customer_bk, payment_date, payment_amount, payment_type_bk
from src
