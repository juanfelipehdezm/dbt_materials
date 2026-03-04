"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""

from dagster import schedule, RunRequest, define_asset_job
from dagster_dbt import build_schedule_from_dbt_selection

from .assets import airbnb_dbt_assets, airbnb_partitioned_dbt_assets, daily_partitions

# Job for partitioned assets
partitioned_asset_job = define_asset_job(
    name="partitioned_dbt_job",
    selection=[airbnb_partitioned_dbt_assets],
    partitions_def=daily_partitions,
)

# Schedule that runs all missing partitions daily at midnight
@schedule(cron_schedule="0 0 * * *", job=partitioned_asset_job)
def daily_partition_backfill_schedule(context):
    """Runs all missing partitions from the last materialized partition to yesterday."""
    # This will automatically materialize all missing partitions
    # If you want to limit to just the latest partition, you can customize the logic here
    return RunRequest()

schedules = [
    build_schedule_from_dbt_selection(
        [airbnb_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        dbt_select="fqn:*",
    ),
    daily_partition_backfill_schedule,
]
