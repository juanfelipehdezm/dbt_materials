{% macro variables() %}
    --jinja variables are only available within the macro where they are defined. They cannot be accessed outside of the macro.
    {% set my_variable = "Juan" %}

    {{ log("The value of my_variable is: " ~ my_variable, info = true) }}
    --dbt variables are defined in the dbt_project.yml file and can be accessed using the var() function. They can also have a default value if the variable is not set.
    {{ log("Hello dbt user " ~ var("user_name", "NO USERNAME IS SET!!") ~ "!", info=True) }}

    {% if var("in_test", False) %}
       {{ log("In test", info=True) }}
    {% else %}
       {{ log("NOT in test", info=True) }}
    {% endif %}

{% endmacro %}