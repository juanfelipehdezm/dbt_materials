{{
    config(
        materialized='view',
        tags=['dim', 'hosts']
    )
}}
-- the config overrides the default materialization set in dbt_project.yml for this specific model

WITH src_hosts AS (
    SELECT * FROM {{ref('src_hosts')}} -- This is a DBT specific jinja template tag to reference another model
)

SELECT 
    host_id,
    COALESCE(host_name, 'Anonymous') AS host_name,
    is_superhost,
    created_at,
    updated_at
FROM 
    src_hosts