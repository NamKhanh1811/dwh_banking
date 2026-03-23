{{ config(materialized='table') }}

select
    {{ generate_surrogate_key(['account_id']) }} as account_key,

    a.account_id,

    sc.customer_key,

    a.account_number,
    a.product_code,

    coalesce(p.product_name, 'Unknown') as product_name,

    {{decode_status('a.account_type', 'account_type')}} as account_type_desc,

    a.currency_code,

    coalesce(p.interest_rate, 0) as interest_rate,

    a.open_date,
    a.close_date,

    case
        when a.close_date is not null
            then (a.close_date - a.open_date)
        else (current_date - a.open_date)
    end as account_age_days,

    {{ decode_status('a.status', 'account_status') }} as status_desc,

    sb.branch_key,
    sb.branch_name,

    current_timestamp as etl_loaded_at

from {{ source('stg', 'stg_account') }} a

left join {{ source('stg', 'stg_product') }} p
    on a.product_code = p.product_code

inner join {{ ref('sor_customer') }} sc
    on a.customer_id = sc.customer_id
   and sc.is_current = true

inner join {{ ref('sor_branch') }} sb
    on a.branch_id = sb.branch_id
order by account_id