# -*- coding: utf-8 -*-
import mlflow
import warnings
from os import environ
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

add_codes_to_datasets = __import__('1_make_all_datasets_coded')
concat_datasets = __import__('2_concat_by_filename')
make_joined_datasets = __import__('3_make_joined_datasets')

warnings.filterwarnings('ignore')


def make_interim_data(region, year, aggr):
    add_codes_to_datasets.run(region=region,
                              year=year,
                              aggr=aggr)


def make_processed_data(region, year, aggr):
    with mlflow.start_run():
        mlflow.log_param("region", region)
        mlflow.log_param("year", year)
        mlflow.log_param("aggr", aggr)

        concat_datasets.run(region=region,
                            year=year,
                            aggr=aggr)
        make_joined_datasets.run(region=region,
                                 year=year,
                                 aggr=aggr)

        mlflow.log_metric("Done", 1)


if __name__ == '__main__':
    # Set Parameters
    region = 'RS'
    year = '2010'
    aggr = 'weighting_area'
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get election results path
    path = 'file:' + project_dir + environ.get("{}_EXPERIMENTS".format(region))
    # Set mflow log dir
    mlflow.set_tracking_uri(path)
    try:
        mlflow.create_experiment('Make census data sets')
    except:
        mlflow.set_experiment('Make census data sets')

    # Set executions
    make_interim = True
    make_processed = True
    if make_interim:
        make_interim_data(region, year, aggr)
    if make_processed:
        make_processed_data(region, year, aggr)
