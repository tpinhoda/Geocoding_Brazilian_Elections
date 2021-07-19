# -*- coding: utf-8 -*-
import mlflow
import warnings
from os import environ
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

concatenate_meshblocks = __import__("1_concat_meshblocks")
dissolve_meshblocks = __import__("2_dissolve_meshblocks")
generate_adj_matrix = __import__("3_generate_adjacency_matrix")

warnings.filterwarnings("ignore")


def make_data(region, year, aggr):
    with mlflow.start_run():
        mlflow.log_param("region", region)
        mlflow.log_param("year", year)
        mlflow.log_param("aggregate", aggr)

        concatenate_meshblocks.run(region, year)
        dissolve_meshblocks.run(region, year, aggr)
        generate_adj_matrix.run(region, year, aggr)

        mlflow.log_metric("Done", 1)


if __name__ == "__main__":
    # Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename="data.env")
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    region_name = environ.get("REGION_NAME")
    census_year = environ.get("CENSUS_YEAR")
    aggr = environ.get("AGGREGATION_LEVEL")
    # Get election results path
    path = "file:" + project_dir + environ.get("Logs").format(region_name)
    # Set mlflow log dir
    mlflow.set_tracking_uri(path)
    try:
        mlflow.create_experiment("Make meshblocks data sets")
    except:
        mlflow.set_experiment("Make meshblocks data sets")
    # Set executions
    make_data(region_name, census_year, aggr)
