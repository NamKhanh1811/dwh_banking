with stg_count as(
    select count(*) as cnt_stg from {{ source('stg', 'stg_customer') }}
),

sor_count as (
    select count(*) as cnt_sor from {{ ref('sor_customer')}} where is_current = TRUE
)

select st.cnt_stg, 
       s.cnt_sor, 
       (st.cnt_stg - s.cnt_sor) as diff,
       'Lỗi' as error_message
from stg_count st 
cross join sor_count s
where (st.cnt_stg - s.cnt_sor) <> 0