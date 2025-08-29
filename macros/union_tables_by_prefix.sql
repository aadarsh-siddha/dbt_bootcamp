{% macro union_tables_by_prefix(database, schema, prefix) %}
    {% set tables=dbt_utils.get_relations_by_pattern(database=database, schema_pattern=schema, table_pattern=prefix) %}
    {% for table in tables %}
        {% if not loop.first %}
            union all            
        {% endif %}
        select * from {{ table.databse }}.{{ table.schema }}.{{ table.name }}
    {% endfor %}
{% endmacro %}