select account_id from {{ source('stg', 'stg_account') }}
except 
select account_id from {{ ref('sor_account') }} 