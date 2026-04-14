{% macro generate_surrogate_key(field_list) %}
    SHA2(CONCAT_WS('||', {% for f in field_list %}COALESCE(CAST({{ f }} AS VARCHAR), '__NULL__'){% if not loop.last %}, {% endif %}{% endfor %}), 256)
{% endmacro %}
