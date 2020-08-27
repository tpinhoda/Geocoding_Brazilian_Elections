# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import environ, mkdir
from os.path import join
from dotenv import load_dotenv, find_dotenv
from tqdm import tqdm
warnings.filterwarnings('ignore')


def neighbors_to_remove(spatial_attr, area, indexes, matrix, data):
    area_matrix = matrix.loc[indexes]
    neighbors = area_matrix.sum(axis=0) > 0
    neighbors = neighbors[neighbors == True].index.astype('int64')
    neighbors_data = data.loc[neighbors]
    to_remove = neighbors_data[neighbors_data[spatial_attr] != area].index
    return to_remove


def make_folds(matched_path, adj_path, spatial_attr):
    output_path = create_folder(matched_path, 'folds_by_'+spatial_attr.split('_')[1])
    adj_m = pd.read_csv(adj_path)
    matched_data = pd.read_csv(join(matched_path, 'census_data', 'data.csv'), index_col=adj_m.columns.values[0])
    adj_m.set_index(adj_m.columns.values[0], inplace=True)

    index_to_remove = matched_data[pd.isnull(matched_data).any(axis=1)].index
    matched_data.drop(index_to_remove)
    adj_m.drop(index_to_remove, axis=0)
    adj_m.drop(index_to_remove.astype(str), axis=1)
    for city, test_data in tqdm(matched_data.groupby(by=spatial_attr)):
        fold_path = create_folder(output_path, str(city).lower())
        train_data = matched_data.copy()
        train_data.drop(test_data.index, inplace=True)
        buffer = neighbors_to_remove(spatial_attr, city, test_data.index, adj_m, matched_data)
        train_data.drop(buffer, inplace=True)
        test_data.to_csv(join(fold_path, 'test.csv'))
        train_data.to_csv(join(fold_path, 'train.csv'))


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


def run(region, tse_year, tse_office, tse_turn, tse_per, ibge_year, ibge_aggr, fold_attr):
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

    make_folds(input_filepath_matched, input_filepath_adj, fold_attr)


#run(region='RS',
#    tse_year='2018',
#    tse_office='president',
#    tse_turn='turn_2',
#    tse_per='PER_99.33031',
#    ibge_year='2010',
#    ibge_aggr='weighting_area',
#    fold_attr='Cod_Municipio')