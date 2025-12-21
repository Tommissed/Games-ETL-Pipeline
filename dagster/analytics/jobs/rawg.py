from analytics.ops.rawg import extract_rawg, transform_rawg, load_rawg
from dagster import job

@job
def run_rawg_etl(): 
    raw_games = extract_rawg()
    load_rawg(transform_rawg(raw_games))