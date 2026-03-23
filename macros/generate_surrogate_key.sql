{% macro generate_surrogate_key(column_names) %} 

MD5(
    {%if column_names | length == 1 %}
        COALESCE(CAST({{column_names[0]}} AS TEXT), 'NULL')
    {%else%}
        CONCAT_WS('|',
            {% for column_name in column_names %}
            COALESCE(CAST({{column_name}} AS TEXT), 'NULL'){%if not loop.last %},{% endif %}
            {% endfor %}
        )
    {% endif %}
)
{% endmacro %}