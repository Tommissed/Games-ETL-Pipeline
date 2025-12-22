from dagster import ScheduleDefinition

from analytics.jobs.rawg import run_rawg_etl

rawg_schedule = ScheduleDefinition(
    name="rawg_schedule",
    job_name="run_rawg_etl",
    cron_schedule="0 * * * *",
)
