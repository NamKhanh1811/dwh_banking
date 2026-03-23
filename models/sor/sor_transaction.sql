{{ config(materialized='table') }}

with src_transaction as (
  select * from {{ source('stg', 'stg_transaction')}}
),

src_account as (
  select * from {{ source('stg', 'stg_account')}}
),

dim_account as (
  select * from {{ ref('sor_account') }}
)


select 
    {{ generate_surrogate_key(['t.transaction_id']) }} as transaction_key,

    t.transaction_id as transaction_id,

    da.account_key as account_key,

    a.account_number as account_number,

    da.customer_key as customer_key,

    t.transaction_date as transaction_date,

    {{ decode_status('t.transaction_type', 'transaction_type') }} as transaction_type_desc,

    t.amount as amount,

    {{ convert_to_vnd('t.amount', 't.currency_code', 'er.mid_rate') }} as amount_vnd,

    t.currency_code as currency_code,

    {{ get_exchange_rate('t.currency_code', 'er.mid_rate') }} as exchange_rate,

    t.description as description,

    {{ decode_status('t.channel', 'channel') }} as channel_desc,

    t.reference_number as reference_number,

    {{ etl_time_stamp() }} as etl_loaded_at
from src_transaction t
inner join src_account a
    on t.account_id = a.account_id
inner join dim_account da
    on a.account_id = da.account_id
{{ build_exchange_rate_join('er', 't.transaction_date', 't.currency_code') }}