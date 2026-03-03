# dbt Dagster Airbnb Project

This folder contains the Dagster orchestration layer for the dbt Airbnb project.

## Structure

- `src/definitions.py` - Main Dagster definitions file
- `src/assets.py` - dbt asset definitions
- `src/project.py` - dbt project configuration
- `src/schedules.py` - Scheduling configuration (currently disabled)

## dbt Project Integration

This Dagster project orchestrates the dbt models located in the `../airbnb` directory.

## Running Dagster

To run the Dagster UI locally:

```bash
dagster dev
```

This will start the Dagster web server and allow you to visualize and run your dbt models through Dagster.

## Configuration

The Dagster configuration is defined in the root `pyproject.toml` under `[tool.dagster]`:
- Module: `dbt_dagster_airbnb_project.definitions`
- Code location: `dbt_dagster_airbnb_project`
