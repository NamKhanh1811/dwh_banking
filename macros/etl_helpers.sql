{% macro etl_time_stamp() %}
    current_timestamp
{% endmacro %}

{% macro etl_batch_date() %}
    current_date
{% endmacro %}

{% macro safe_divide(numerator, denominator, default_val = NULL) %}
    case
        when {{denominator}} = 0 then {{default_val}}
        else {{numerator}} / {{denominator}}
    end
{% endmacro %}

{% macro coalesce_zero(col) %}
    coalesce({{col}}, 0)
{% endmacro %}

{%macro log_model_start(model_name) %}
    {{ log('>>> Đang compile: ' ~ model_name, info=True) }}
{% endmacro %}


