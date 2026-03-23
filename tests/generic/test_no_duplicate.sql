{% test no_duplicate(model, column_name) %}
    select {{ column_name}}, count(*) as occurrences
    from {{model}}
    where {{ column_name }} is not null
    group by {{ column_name }}
    having count(*) > 1

{% endtest %}