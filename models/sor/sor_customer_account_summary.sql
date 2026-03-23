{{ config(materialized='table') }}

with latest_balance as (

    select *
    from (
        select *,
            row_number() over(
                partition by account_key
                order by balance_date desc
            ) as rn
        from {{ ref('sor_daily_balance') }}
    ) t
    where rn = 1

)

select
    {{ generate_surrogate_key(['c.customer_key']) }} as summary_key,

    c.customer_key,
    c.customer_id,
    c.full_name,

    count(distinct a.account_key) as total_accounts,

    count(distinct case when a.status_desc = 'Active' then a.account_key end)
        as total_active_accounts,

    count(distinct case when a.status_desc = 'Closed' then a.account_key end)
        as total_closed_accounts,

    sum(lb.closing_balance_vnd) as total_balance_vnd,

    avg(lb.closing_balance_vnd) as avg_balance_vnd,

    sum(case when t.transaction_type_desc = 'Credit'
             then t.amount_vnd else 0 end)
        as total_credit_amount_vnd,

    sum(case when t.transaction_type_desc = 'Debit'
             then t.amount_vnd else 0 end)
        as total_debit_amount_vnd,

    count(distinct t.transaction_key) as total_transactions,

    min(a.open_date) as earliest_open_date,

    max(t.transaction_date) as latest_transaction_date,

    (
        select a2.branch_name
        from {{ ref('sor_account') }} a2
        where a2.customer_key = c.customer_key
        group by a2.branch_name
        order by count(*) desc
        limit 1
    ) as primary_branch_name,

    current_timestamp as etl_loaded_at

from {{ ref('sor_customer') }} c

left join {{ ref('sor_account') }} a
    on c.customer_key = a.customer_key

left join {{ ref('sor_transaction') }} t
    on c.customer_key = t.customer_key

left join latest_balance lb
    on a.account_key = lb.account_key

where c.is_current = true

group by
    c.customer_key,
    c.customer_id,
    c.full_name