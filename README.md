# dbt_materials
dbt materials for study purposes

## Project Structure

- `airbnb/` - dbt project for Airbnb data pipeline
- `dbt_dagster_airbnb_project/` - Dagster orchestration layer
- `database_configuration/` - Database setup and configuration files
- `pyproject.toml` - Python project dependencies (shared)

## Components

### dbt Project (airbnb/)
Contains the dbt models, tests, macros, and configurations for the Airbnb data pipeline.

### Dagster Orchestration (dbt_dagster_airbnb_project/)
Dagster integration that orchestrates the dbt models. See [dbt_dagster_airbnb_project/README.md](dbt_dagster_airbnb_project/README.md) for details.

## Setup

Install dependencies:
```bash
uv sync
```

Run Dagster:
```bash
dagster dev
# or with uv:
uv run dagster dev
```

Run dbt:
```bash
cd airbnb
dbt run
```
