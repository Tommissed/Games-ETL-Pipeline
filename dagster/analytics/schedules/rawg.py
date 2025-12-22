from dagster import build_schedule_from_partitioned_job

from analytics.jobs.rawg import run_rawg_etl

rawg_schedule = build_schedule_from_partitioned_job(
    job=run_rawg_etl
)
