# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import environ, mkdir
from os.path import join
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def merge_datasets(matched_path, census_path):
    strangeness = pd.read_csv(join(matched_path, 'election_results', 'strangeness.csv'))
    census_data = pd.read_csv(census_path)
    merged_data = census_data.merge(strangeness, on='Cod_ap', how='left')
    output_path = create_folder(matched_path, 'census_data')
    merged_data.to_csv(join(output_path, 'data.csv'), index=False)


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


def run(region, tse_year, tse_office, tse_turn, tse_per, ibge_year, ibge_aggr):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path_census = data_dir + environ.get('{}_CENSUS_DATA'.format(region))
    path_matched = data_dir + environ.get('{}_MATCHED_DATA'.format(region))
    # Generate input output paths
    joined_data_path = path_matched
    processed_path_census = path_census.format(ibge_year, 'processed')
    # Set paths
    # Strangeness path
    input_filepath_matched = join(joined_data_path, 'tse{}_ibge{}'.format(tse_year, ibge_year))
    input_filepath_matched = join(input_filepath_matched, '{}_{}'.format(tse_office, tse_turn))
    input_filepath_matched = join(input_filepath_matched, ibge_aggr, tse_per)
    # Census data path
    input_filepath_census = join(processed_path_census, ibge_aggr, 'joined_all', 'data.csv')
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    merge_datasets(input_filepath_matched, input_filepath_census)


#run(region='RS',
#    tse_year='2018',
#    tse_office='president',
#    tse_turn='turn_2',
#    tse_per='PER_99.33031',
#    ibge_year='2010',
#    ibge_aggr='weighting_area')