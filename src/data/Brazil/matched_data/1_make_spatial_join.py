# -*- coding: utf-8 -*-
import warnings
import logging
import geopandas as gpd
import pandas as pd
from os import listdir, environ, mkdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def make_spatial_join(input_path_tse, input_path_meshblocks, output_path):
    data_tse = pd.read_csv(input_path_tse)
    meshblocks = gpd.read_file(input_path_meshblocks)
    print(data_tse.head())

def create_folder(path, folder_name):
    logger = logging.getLogger(__name__)
    path = join(path, folder_name)
    exist = 0
    try:
        mkdir(path)
    except FileExistsError:
        logger.info('Folder already exist.')
        exist = 1
    return path


def run(region, tse_year, tse_office, tse_turn, tse_aggr, tse_per, ibge_year, ibge_aggr):
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get census results path
    path_tse = project_dir + environ.get('{}_ELECTION_RESULTS'.format(region))
    path_meshblocks = project_dir + environ.get('{}_CENSUS_DATA'.format(region))
    path_matched = project_dir + environ.get('{}_MATCHED_DATA'.format(region))
    # Generate input output paths

    processed_path_tse = path_tse.format(tse_year, 'processed')
    processed_path_meshblocks = path_meshblocks.format(ibge_year, 'processed')
    # Set paths
    input_filepath_tse = join(processed_path_tse, tse_office, tse_turn, tse_aggr, tse_per, 'data.csv')
    input_filepath_meshblocks = join(processed_path_meshblocks, ibge_aggr, 'shapefiles', region + '.shp')
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    # Create output root folder
    logger.info('Creating root folder.')
    folder = 'tse{}_ibge{}'.format(tse_year, ibge_year)
    output_filepath = create_folder(path_matched, folder)
    # Create aggregation folder
    logger.info('Creating aggregation folder.')
    folder = ibge_aggr
    output_filepath = create_folder(output_path, folder)
    # Create PER folder
    logger.info('Creating PER folder.')
    folder = tse_per
    output_filepath = create_folder(output_path, folder)
    # Create election results folder
    logger.info('Creating election results folder.')
    folder = 'election_results'
    output_filepath = create_folder(output_path, folder)

    make_spatial_join(input_filepath_tse, input_filepath_meshblocks, output_filepath)


run(region='RS',
    tse_year='2018',
    tse_office='president',
    tse_turn='turn_2',
    tse_aggr='aggr_by_polling_place',
    tse_per='')
