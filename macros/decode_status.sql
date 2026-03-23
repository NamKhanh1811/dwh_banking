{% macro decode_status(column_name, type) %}
{%- if type == 'account_status' -%}
    case 
        when {{ column_name }} = 'A' then 'Active'
        when {{ column_name }} = 'C' then 'Closed'
        when {{ column_name }} = 'D' then 'Dormant'
        when {{ column_name }} = 'F' then 'Frozen'
        else 'Unknown'
    end
{%- elif type == 'branch_status' -%}
    case 
        when {{ column_name }} = 'A' then 'Active'
        when {{ column_name }} = 'C' then 'Closed'
        else 'Unknown'
    end
{%- elif type == 'customer_type' -%}
    case
        when {{ column_name }} = 'I' then 'Cá nhân'
        when {{ column_name }} = 'B' then 'Doanh nghiệp'
        else 'Khác'
    end
{%- elif type == 'id_type' -%}
    case 
        when {{column_name}} = 'CC' then 'CCCD'
        when {{column_name}} = 'PP' then 'Hộ chiếu'
        when {{column_name}} = 'CMT' then 'CMND'
        when {{column_name}} = 'BL' then 'Giấy phép KD'
    end
{%- elif type == 'transaction_type' -%}
    case
        when {{ column_name }} = 'CR' then 'Credit'
        when {{ column_name }} = 'DR' then 'Debit'
    end
{%- elif type == 'channel' -%}
    case 
        when {{ column_name }} = 'ATM'     then 'ATM'
        when {{ column_name }} = 'POS'     then 'POS'
        when {{ column_name }} = 'IB'      then 'Internet Banking'
        when {{ column_name }} = 'MB'      then 'Mobile Banking'
        when {{ column_name }} = 'COUNTER' then 'Quầy'
        else 'Khác'
    end
{%- elif type == 'account_type' -%}
    case
        when {{ column_name }} = 'SA' then 'Saving'
        when {{ column_name }} = 'CA' then 'Checking'
        when {{ column_name }} = 'TD' then 'Credit'
        when {{ column_name }} = 'LN' then 'Loan'
    end
{%- elif type == 'gender' -%}
    case
        when {{ column_name }} = 'M' then 'Nam'
        when {{ column_name }} = 'F' then 'Nữ'
        when {{ column_name }} is null then 'N/A'
    end 
{%- elif type == 'product_category' -%}
    case 
        when {{ column_name }} = 'SAVINGS' then 'Tiết kiệm'
        when {{ column_name }} = 'CURRENT' then 'Thanh toán'
        when {{ column_name }} = 'LOAN' then 'Vay'
        when {{ column_name }} = 'CARD' then 'Thẻ'
        else 'Khác'
    end
{%- else -%}
    {{ column_name }} /* type ko khớp '{{ type }}' */
{%- endif -%}
{% endmacro %}