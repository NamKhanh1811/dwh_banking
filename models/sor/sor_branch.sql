{{ config(materialized='table') }}

select
    {{ generate_surrogate_key(['branch_id']) }} as branch_key,

    branch_id,

    upper(branch_name) as branch_name,

    branch_code,
    region,
    city,

    trim(address) as address,

    manager_name,

    {{ decode_status('status', 'branch_status') }} as status_desc,

    {{ etl_time_stamp() }} as etl_loaded_at

from {{ source('stg', 'stg_branch') }}