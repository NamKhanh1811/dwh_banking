with balance_check as (
    select *,
           (closing_balance_vnd < 0) as rule1_fail,
           (exchange_rate <= 0) as rule2_fail,
           (currency_code = 'VND' and exchange_rate <> 1) as rule3_fail
    from {{ ref('sor_daily_balance') }}
)

select account_number,
       balance_date,
       currency_code,
       closing_balance_vnd,
       exchange_rate, 
       case
        when rule1_fail then 'fail rule1'
        when rule2_fail then 'fail rule2'
        when rule3_fail then 'fail rule3' 
       end as violated_rule
from balance_check
where rule1_fail or rule2_fail or rule3_fail