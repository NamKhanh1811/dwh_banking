{{ config(materialized='table') }}

select
    {{ generate_surrogate_key(['b.account_id', 'b.balance_date']) }} as balance_key,

    sa.account_key as account_key,

    sa.account_number as account_number,

    sa.customer_key as customer_key,

    b.balance_date as balance_date,

    b.opening_balance as opening_balance,

    b.closing_balance as closing_balance,

    CASE
      WHEN b.currency_code = 'VND' THEN b.opening_balance
      ELSE b.opening_balance * COALESCE(er.mid_rate, 0)
    END as opening_balance_vnd,

    CASE
      WHEN b.currency_code = 'VND' THEN b.closing_balance
      ELSE b.closing_balance * COALESCE(er.mid_rate, 0)
    END as closing_balance_vnd,

    b.closing_balance - b.opening_balance as net_movement,

    CASE
      WHEN b.currency_code = 'VND'
      THEN b.closing_balance - b.opening_balance
      ELSE (b.closing_balance - b.opening_balance) * COALESCE(er.mid_rate, 0)
    END as net_movement_vnd,

    b.currency_code as currency_code,

    CASE
      WHEN b.currency_code = 'VND' THEN 1
      ELSE COALESCE(er.mid_rate, 0)
    END as exchange_rate, 

    current_timestamp as etl_loaded_at
from {{ source('stg', 'stg_account_balance') }} b
inner join {{ ref('sor_account') }} sa
    on b.account_id = sa.account_id
left join {{ source('stg', 'stg_exchange_rate') }} er
    on b.currency_code = er.currency_from AND er.currency_to = 'VND' AND b.balance_date = er.rate_date