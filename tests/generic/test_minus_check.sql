{% test minus_check(model, column_name, source_table, source_column) %}

select
    {{ source_column }}
from {{ source_table }}

except

select
    {{ column_name }}
from {{ model }}

{% endtest %}