{% macro variables() %}

    {% set my_variable = "Juan" %}

    {{ log("The value of my_variable is: " ~ my_variable, info = true) }}
    
    {{ log("Hello dbt user " ~ var("user_name", "NO USERNAME IS SET!!") ~ "!", info=True) }}

    {% if var("in_test", False) %}
       {{ log("In test", info=True) }}
    {% else %}
       {{ log("NOT in test", info=True) }}
    {% endif %}

{% endmacro %}