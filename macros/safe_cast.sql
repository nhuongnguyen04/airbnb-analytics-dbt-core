-- macros/safe_cast.sql

{% macro safe_cast(expression, target_type) %}
 {#-- Kiểm tra và ép kiểu an toàn cho các loại dữ liệu phổ biến --#}

    {%- if target_type in ['int', 'integer', 'bigint'] -%}
        -- Ép kiểu số nguyên nếu biểu thức chỉ chứa chữ số
        CASE 
            WHEN {{ expression }}::text ~ '^[0-9]+$' 
            THEN CAST({{ expression }} AS INTEGER)
            ELSE NULL
        END
    {%- elif target_type in ['numeric', 'decimal', 'float', 'double precision'] -%}
        -- Ép kiểu số thực nếu biểu thức chỉ chứa số và dấu thập phân
        CASE 
            WHEN {{ expression }}::text ~ '^[0-9]+(\.[0-9]+)?$' 
            THEN CAST({{ expression }} AS {{ target_type }})
            ELSE NULL
        END
    {%- elif target_type in ['date'] -%}
        -- Ép kiểu ngày nếu biểu thức có định dạng YYYY-MM-DD
        CASE 
            WHEN {{ expression }}::text ~ '^\d{4}-\d{2}-\d{2}$' 
            THEN CAST({{ expression }} AS DATE)
            ELSE NULL
        END
    {%- elif target_type in ['timestamp'] -%}
        -- Ép kiểu timestamp nếu biểu thức có định dạng YYYY-MM-DD HH:MM:SS
        CASE 
            WHEN {{ expression }}::text ~ '^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$' 
            THEN CAST({{ expression }} AS TIMESTAMP)
            ELSE NULL
        END
    {%- elif target_type in ['boolean', 'bool'] -%}
        -- Ép kiểu boolean nếu biểu thức có giá trị hợp lệ
        CASE 
            WHEN lower({{ expression }}::text) in ('true','false','t','f','yes','no','1','0') 
            THEN CAST({{ expression }} AS BOOLEAN)
            ELSE NULL
        END
    {%- else -%}
        -- Ép kiểu mặc định cho các loại dữ liệu khác
        CAST({{ expression }} AS {{ target_type }})
    {%- endif -%}
{% endmacro %}
