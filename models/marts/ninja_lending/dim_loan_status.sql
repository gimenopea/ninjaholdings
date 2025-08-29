{{ config(materialized='table') }}

with s as (
  select 'ACTIVE'    as loan_status_bk, 'Active'    as loan_status union all
  select 'PAID',     'Paid'                             union all
  select 'DEFAULTED','Defaulted'
)
select
  {{ sk(['loan_status_bk']) }} as loan_status_sk,
  loan_status_bk,
  loan_status
from s
