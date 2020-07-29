# -*- coding: utf-8 -*-
import mlflow
import pandas as pd
import warnings
from ast import literal_eval
from os import environ
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

make_political_office = __import__('1_make_political_office_dataset')
merge_datasets = __import__('2_merge_political_office_ds_with_polling_place_ds')
clean_dataset = __import__('3_clean_ds_by_per')
generate_statistics = __import__('4_make_ds_statistics')

warnings.filterwarnings('ignore')


def make_interim_data(region, params):
    params = params.drop_duplicates(subset=['year', 'political_office', 'office_folder', 'turn'],
                                    keep='first')
    for index, parameters in params.iterrows():
        make_political_office.run(region=region,
                                  year=parameters.year,
                                  political_office=parameters.political_office,
                                  office_folder=parameters.office_folder,
                                  turn=parameters.turn)

        merge_datasets.run(region=region,
                           year=parameters.year,
                           office_folder=parameters.office_folder,
                           turn=parameters.turn)


def make_processed_data(region, params):
    for index, parameters in params.iterrows():
        with mlflow.start_run():
            mlflow.log_param("region", region)
            mlflow.log_param("year", parameters.year)
            mlflow.log_param("political_office", parameters.political_office)
            mlflow.log_param("turn", parameters.turn)
            mlflow.log_param("candidates", parameters.candidates)
            mlflow.log_param("city_limits", parameters.city_limits)
            mlflow.log_param("levenshtein_threshold", parameters.levenshtein_threshold)
            mlflow.log_param("precision_categories", parameters.precision_categories)
            mlflow.log_param("aggregate_level", parameters.aggregate_level)

            per = clean_dataset.run(region=region,
                                    year=parameters.year,
                                    office_folder=parameters.office_folder,
                                    turn=parameters.turn,
                                    candidates=parameters.candidates,
                                    city_limits=parameters.city_limits,
                                    levenshtein_threshold=parameters.levenshtein_threshold,
                                    precision_categories=parameters.precision_categories,
                                    aggregate_level=parameters.aggregate_level)

            is_score = generate_statistics.run(region=region,
                                               year=parameters.year,
                                               office_folder=parameters.office_folder,
                                               turn=parameters.turn,
                                               candidates=parameters.candidates,
                                               city_limits=parameters.city_limits,
                                               levenshtein_threshold=parameters.levenshtein_threshold,
                                               precision_categories=parameters.precision_categories,
                                               aggregate_level=parameters.aggregate_level,
                                               per=per)
            mlflow.log_metric("PER", float(per))
            mlflow.log_metric("IS", is_score)


if __name__ == '__main__':
    #Set Region
    region = 'RS'
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
        mlflow.create_experiment('Make election results data sets')
    except:
        mlflow.set_experiment('Make election results data sets')

    param_sets = pd.read_csv('parameters_to_generate_2018_ds.csv', converters={'candidates': literal_eval,
                                                                               'city_limits': literal_eval,
                                                                               'precision_categories': literal_eval})

    # Set executions
    make_interim = False
    make_processed = True
    if make_interim:
        make_interim_data(region, param_sets)
    if make_processed:
        make_processed_data(region, param_sets)
