# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import environ, mkdir
from os.path import join
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm
warnings.filterwarnings('ignore')


def run(region, tse_year, tse_office, tse_turn, tse_per, ibge_year, ibge_aggr, fold_name):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path_meshblocks = data_dir + environ.get('{}_MESHBLOCKS'.format(region))
    path_matched = data_dir + environ.get('{}_MATCHED_DATA'.format(region))
    # Generate input output paths
    joined_data_path = path_matched
    processed_path_meshblocks = path_meshblocks.format(ibge_year, 'processed')
    # Set paths
    # Strangeness path
    input_filepath_matched = join(joined_data_path, 'tse{}_ibge{}'.format(tse_year, ibge_year))
    input_filepath_matched = join(input_filepath_matched, '{}_{}'.format(tse_office, tse_turn))
    input_filepath_matched = join(input_filepath_matched, ibge_aggr, tse_per)
    # Adjacency matrix data path
    input_filepath_adj = join(processed_path_meshblocks, ibge_aggr, 'adjacency_matrix.csv')
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    print(input_filepath_matched)
    print(input_filepath_adj)


run(region='RS',
    tse_year='2018',
    tse_office='president',
    tse_turn='turn_2',
    tse_per='PER_99.33031',
    ibge_year='2010',
    ibge_aggr='weighting_area',
    fold_name='Cod_Municipio')
