{{ config(materialized='table') }}

select
    {{ generate_surrogate_key(['customer_id']) }} as customer_key,

    customer_id,

    upper(trim(customer_name)) as full_name,

    {{ decode_status('gender', 'gender')}} as gender_desc,

    date_of_birth,

    case
        when date_of_birth is not null
        then extract(year from age(current_date, date_of_birth))::integer
        else null
    end as age,

    id_number,

    {{ decode_status('id_type', 'id_type')}} as id_type_desc,

    phone,

    lower(trim(email)) as email,

    {{ decode_status('customer_type', 'customer_type') }} as customer_type_desc,

    registration_date,

    current_date as effective_from,
    cast('9999-12-31' as date) as effective_to,
    true as is_current,

    current_timestamp as etl_loaded_at

from {{ source('stg', 'stg_customer') }}