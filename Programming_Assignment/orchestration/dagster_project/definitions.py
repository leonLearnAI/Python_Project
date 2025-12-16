from dagster import Definitions
from orchestration.dagster_project.assets import ingest_to_postgres, create_analytics_views, export_sql_outputs

defs = Definitions(
    assets=[ingest_to_postgres, create_analytics_views, export_sql_outputs]
)