# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import environ
from os.path import join
from dotenv import load_dotenv, find_dotenv
import numpy as np
from scipy.spatial import distance

warnings.filterwarnings('ignore')


def calculate_cityblock_matrix(data, candidate):
    data[candidate] = data[candidate]/data['QT_COMPARECIMENTO']
    candidate_votes = np.array(data[candidate].values).reshape(-1, 1)
    votes_sm = distance.pdist(candidate_votes, 'cityblock')
    votes_sm = pd.DataFrame(distance.squareform(votes_sm), index=data.index, columns=data.index)
    return votes_sm


def calculate_strangeness(path_tse, input_path_adj_matrix, candidate, dist_func='city_block'):
    hierarchies = ['NM_MICRO', 'NM_MESO', 'NM_UF']
    hierarchies_strangeness = dict()
    data = pd.read_csv(join(path_tse, 'data.csv'))
    matrix = pd.read_csv(input_path_adj_matrix)

    data.set_index(matrix.columns.values[0], inplace=True)
    data.index = data.index.astype('int64')
    matrix.set_index(matrix.columns.values[0], inplace=True)

    for geo_hierarchy in hierarchies:
        aggregated_data = data.groupby(geo_hierarchy)
        list_strangeness = []
        for geo_hie, region_data in aggregated_data:
            region_matrix = matrix.loc[region_data.index]
            region_matrix = region_matrix[region_data.index.astype(str).values]
            if dist_func == 'city_block':
                votes_matrix = calculate_cityblock_matrix(region_data, candidate)
            adj_matrix = pd.DataFrame(votes_matrix.values*region_matrix.values, columns=region_matrix.index, index=region_matrix.index)
            strangeness = adj_matrix.sum(axis=0)
            list_strangeness.append(strangeness)
        hierarchies_strangeness[geo_hierarchy+'_STRANGENESS'] = pd.concat(list_strangeness, axis=0)

    df_strangeness = pd.concat(hierarchies_strangeness, axis=1)
    df_strangeness.to_csv(join(path_tse, 'strangeness.csv'))


def run(region, tse_year, tse_office, tse_turn, tse_per, candidates, ibge_year, ibge_aggr):
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
    input_filepath_tse = join(joined_data_path, 'tse{}_ibge{}'.format(tse_year, ibge_year))
    input_filepath_tse = join(input_filepath_tse, '{}_{}'.format(tse_office, tse_turn))
    input_filepath_tse = join(input_filepath_tse, ibge_aggr, tse_per, 'election_results')
    input_filepath_adj_matrix = join(processed_path_meshblocks, ibge_aggr, 'adjacency_matrix.csv')
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    logger = logging.getLogger(__name__)

    calculate_strangeness(input_filepath_tse, input_filepath_adj_matrix, candidate=candidates[0])


#run(region='RS',
#    tse_year='2018',
#    tse_office='president',
#    tse_turn='turn_2',
#    tse_per='PER_99.33031',
#    candidates=['FERNANDO HADDAD', 'JAIR BOLSONARO'],
#    ibge_year='2010',
#    ibge_aggr='weighting_area')
