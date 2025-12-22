from datetime import datetime

from analytics.ops.rawg import extract_rawg, transform_rawg, load_rawg
from dagster import job, daily_partitioned_config

@daily_partitioned_config(start_date=datetime(2024, 1, 1))
def rawg_daily_partition(start:datetime, _end:datetime):
    return {
        "ops": {
            "extract_rawg": {
                "config": {
                    "date": start.strftime("%Y-%m-%d")
                }
            }
        }
    }

@job(config=rawg_daily_partition)
def run_rawg_etl(): 
    raw_games = extract_rawg()
    load_rawg(transform_rawg(raw_games))