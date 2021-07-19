# -*- coding: utf-8 -*-
import warnings
import logging
import geopandas as gpd
import pandas as pd
from os import environ
from os.path import join
from dotenv import load_dotenv, find_dotenv
from libpysal.weights.contiguity import Queen
from libpysal.weights import DistanceBand

warnings.filterwarnings("ignore")


def generate_adjacency_matrix(input_filepath, output_filepath, region, aggr):
    logger = logging.getLogger(__name__)
    logger.info("Generating adjacency matrix according to: {}".format(aggr))
    if aggr == "sub_dist":
        aggr_attr = "CD_GEOCODS"
    elif aggr == "census_tract":
        aggr_attr = "CD_GEOCODI"
    elif aggr == "dist":
        aggr_attr = "CD_GEOCODD"
    elif aggr == "city":
        aggr_attr = "CD_GEOCMU"
    elif aggr == "weighting_area":
        aggr_attr = "Cod_ap"
    elif aggr == "uf":
        aggr_attr = "NM_UF"
    # Read meshblock
    data = gpd.read_file(join(input_filepath, aggr, "shapefiles", region + ".shp"))
    # Set the aggregation attribute as index
    data.set_index(aggr_attr, inplace=True)
    # Calculate the adjacency matrix according to queen strategy
    weights = Queen.from_dataframe(data)
    # Calculate the adjacency matrix according to inverse distance strategy
    # weights = DistanceBand.from_dataframe(data, threshold=None, build_sp=False, binary=False)
    # Get the adjacency matryx
    w_matrix, ids = weights.full()
    # Associating the aggregating codes as indexes
    w_matrix = pd.DataFrame(w_matrix, index=data.index, columns=data.index)
    # Saving the adjacency matrix as csv
    w_matrix.to_csv(join(output_filepath, aggr, "adjacency_matrix_inv.csv"))


def run(region, year, aggr):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename="data.env")
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get("ROOT_DATA")
    # Get census results path
    path = data_dir + environ.get("MESHBLOCKS")
    # Generate input output paths
    processed_path = path.format(region, year, "processed")
    # Set paths
    input_filepath = processed_path
    output_filepath = processed_path
    # Log text to show on screen
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    # print('======Parameters========')
    # print('Census year: {}'.format(year))

    generate_adjacency_matrix(input_filepath, output_filepath, region, aggr)


run("Brazil", "2010", "uf")
