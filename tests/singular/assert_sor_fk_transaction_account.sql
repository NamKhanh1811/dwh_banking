select t.account_key, 
       'transaction không có account tương ứng' as error_message
from {{ ref('sor_transaction')  }} t
left join {{ ref('sor_account') }} a on t.account_key = a.account_key
where a.account_key is null and t.account_key is not null

union all

select t.customer_key, 
       'transaction không có customer tương ứng' as error_message
from {{ ref('sor_transaction')  }} t
left join {{ ref('sor_customer') }} c on t.customer_key = c.customer_key
where c.customer_key is null and t.customer_key is not null

union all

select d.account_key, 
       'balance record không có account tương ứng' as error_message
from {{ ref('sor_daily_balance')  }} d
left join {{ ref('sor_account') }} a on d.account_key = a.account_key
where a.account_key is null and d.account_key is not null