{% test value_in_range(model, column_name, min_value, max_value) %}
    select {{ column_name }} as invalid_value,
    case 
        when {{ column_name }} < {{min_value}} then 'below_min'
        when {{ column_name }} > {{max_value}} then 'above_max'
    end as violation_reason

    from {{ model}} 
    where {{ column_name }} is not null
    and ({{ column_name }} < {{min_value}} or {{ column_name }} > {{max_value}})

{% endtest %}