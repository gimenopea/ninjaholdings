with src as (
  select
    application_id                             as application_bk,
    customer_id                                as customer_bk,
    try_to_date(application_date)              as application_date,
    /* Oracle NUMBER → Snowflake NUMBER(38,10) to avoid float */
    try_cast(loan_amount_requested as number(38,10)) as loan_amount_requested,
    upper(status)                              as status_bk,
    try_to_timestamp_ntz(convert_timezone('UTC', to_timestamp_tz(updated_at))) as updated_ts
  from {{ source('ninja_lending','loan_applications') }}
)
select
  {{ sk(['application_bk']) }}                 as application_sk,
  application_bk, customer_bk, application_date, loan_amount_requested, status_bk, updated_ts
from src
