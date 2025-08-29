-- Fail if any payment is non-positive
select 1
from {{ ref('fct_payments') }}
where payment_amount <= 0
limit 1
