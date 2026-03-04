from dagster import Definitions
from dagster_dbt import DbtCliResource
from .assets import airbnb_dbt_assets, airbnb_partitioned_dbt_assets
from .project import dbt_project_dir
from .schedules import schedules

defs = Definitions(
    assets=[airbnb_dbt_assets, airbnb_partitioned_dbt_assets],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_dir),
    },
)
