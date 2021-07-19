# -*- coding: utf-8 -*-
import mlflow
import pandas as pd
import warnings
import logging
from ast import literal_eval
from os import environ, mkdir, listdir, rename
from os.path import join, isfile
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

make_political_office = __import__("1_make_political_office_dataset")
merge_datasets = __import__("2_merge_political_office_ds_with_polling_place_ds")
clean_dataset = __import__("3_clean_ds_by_per")
generate_statistics = __import__("4_make_ds_statistics")

warnings.filterwarnings("ignore")


def create_folder(path, folder_name):
    logger = logging.getLogger(__name__)
    path = join(path, folder_name)
    try:
        mkdir(path)
    except FileExistsError:
        logger.info("Folder already exist.")
    return path + "/"


def make_folders(root_path, region, election_year, election_turn, params):
    path = create_folder(root_path, region)
    path = create_folder(path, "TSE")
    path = create_folder(path, str(election_year))
    for type_data in ["raw", "interim", "processed"]:
        step_path = create_folder(path, type_data)
        step_path = create_folder(step_path, "election_results")
        if type_data in ["interim", "processed"]:
            for key, _ in params.groupby("political_office"):
                step_path = create_folder(step_path, key)
                create_folder(step_path, "turn_" + election_turn)
        else:
            raw_path = create_folder(step_path, "turn_" + election_turn)
    return raw_path


def make_interim_data(region, election_year, election_turn, params):
    params = params.drop_duplicates(
        subset=["year", "political_office", "office_folder", "turn"], keep="first"
    )
    for _, parameters in params.iterrows():
        make_political_office.run(
            region=region,
            year=election_year,
            political_office=parameters.political_office,
            office_folder=parameters.political_office,
            turn=election_turn,
        )

        merge_datasets.run(
            region=region,
            year=election_year,
            office_folder=parameters.political_office,
            turn=election_turn,
        )


def make_processed_data(region, election_year, election_turn, params):
    for index, parameters in params.iterrows():
        with mlflow.start_run():
            mlflow.log_param("region", region)
            mlflow.log_param("year", election_year)
            mlflow.log_param("political_office", parameters.political_office)
            mlflow.log_param("turn", election_turn)
            mlflow.log_param("candidates", parameters.candidates)
            mlflow.log_param("city_limits", parameters.city_limits)
            mlflow.log_param("levenshtein_threshold", parameters.levenshtein_threshold)
            mlflow.log_param("precision_categories", parameters.precision_categories)
            mlflow.log_param("aggregate_level", parameters.aggregate_level)

            per = clean_dataset.run(
                region=region,
                year=election_year,
                office_folder=parameters.political_office,
                turn=election_turn,
                candidates=parameters.candidates,
                city_limits=parameters.city_limits,
                levenshtein_threshold=parameters.levenshtein_threshold,
                precision_categories=parameters.precision_categories,
                aggregate_level=parameters.aggregate_level,
            )

            is_score = generate_statistics.run(
                region=region,
                year=election_year,
                office_folder=parameters.political_office,
                turn=election_turn,
                candidates=parameters.candidates,
                city_limits=parameters.city_limits,
                levenshtein_threshold=parameters.levenshtein_threshold,
                precision_categories=parameters.precision_categories,
                aggregate_level=parameters.aggregate_level,
                per=per,
            )
            mlflow.log_metric("PER", float(per))
            mlflow.log_metric("IS", is_score)


if __name__ == "__main__":
    # Log text to show on screen
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    # Find .env automatically by walking up directories until it's found
    dataenv_path = find_dotenv(filename="data.env")
    # Load up the entries as environment variables
    load_dotenv(dataenv_path)
    # Get config variables
    region_name = environ.get("REGION_NAME")
    election_year = environ.get("ELECTION_YEAR")
    election_turn = environ.get("ELECTION_TURN")
    # Reading the filter parameters
    params_path = join(project_dir, "parameters", "single_param_set.csv")
    param_sets = pd.read_csv(
        params_path,
        converters={
            "candidates": literal_eval,
            "city_limits": literal_eval,
            "precision_categories": literal_eval,
        },
    )
    # Make data folders
    logger = logging.getLogger(__name__)
    logger.info("Creating data folders: {}".format(region_name))
    root_folder = environ.get("ROOT_DATA")
    raw_files = filenames = [
        filename
        for filename in listdir(root_folder)
        if isfile(join(root_folder, filename))
    ]

    raw_path = make_folders(
        root_folder, region_name, election_year, election_turn, param_sets
    )
    for file in raw_files:
        rename(join(root_folder, file), join(raw_path, file))

    # Get election results path
    path = "file:" + project_dir + environ.get("LOGS").format(region_name)
    # Set mflow log dir
    mlflow.set_tracking_uri(path)
    try:
        mlflow.create_experiment("Make election results data sets")
    except:
        mlflow.set_experiment("Make election results data sets")

    # Set executions
    make_interim = True
    make_processed = True
    if make_interim:
        make_interim_data(region_name, election_year, election_turn, param_sets)
    if make_processed:
        make_processed_data(region_name, election_year, election_turn, param_sets)
