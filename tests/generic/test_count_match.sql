{% test count_match(model, source_table) %}
with source_count as(
    select count(*) as cnt
    from {{ source_table}}
),

target_count as( 
    select count(*) as cnt
    from {{ model}}
)

select 
    s.cnt as source_count,
    t.cnt as target_count,
    (s.cnt - t.cnt) as diff

from source_count s 
cross join target_count t

where s.cnt <> t.cnt 

{% endtest %}