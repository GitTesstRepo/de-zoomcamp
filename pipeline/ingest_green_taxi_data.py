#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--target-table', default='green_taxi_trips', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table):
    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url_parquet = f"{prefix}/green_tripdata_{year}-{month:02d}.parquet"
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df_parquet = pd.read_parquet(url_parquet)

    df_parquet.to_sql(name=target_table, con=engine, if_exists='replace')

    url_csv = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"
    
    df_csv = pd.read_csv(url_csv)
    df_csv.to_sql(name="taxi_zone_lookup", con=engine, if_exists='replace')

if __name__ == '__main__':
    run()