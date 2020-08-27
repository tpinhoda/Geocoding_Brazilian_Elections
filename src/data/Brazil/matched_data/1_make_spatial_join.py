# -*- coding: utf-8 -*-
import warnings
import logging
import geopandas as gpd
import pandas as pd
from os import environ, mkdir
from os.path import join
from shapely.geometry import Point
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def make_spatial_join(input_path_tse, input_path_meshblocks, output_path):
    data_tse = pd.read_csv(input_path_tse)
    meshblocks = gpd.read_file(input_path_meshblocks)
    geometry = [Point((row.lon, row.lat)) for index, row in data_tse.iterrows()]
    data_tse = gpd.GeoDataFrame(data_tse, geometry=geometry)
    data_tse.crs = "EPSG:4674"
    data_joined = gpd.sjoin(left_df=data_tse, right_df=meshblocks, how='left', op='within')
    #data_joined.to_csv(join(output_path, 'data.csv'), index=False)
    return data_joined


def aggregate_data(data, aggregate_level, candidates):
    aggr_map = {'Cod_ap': 'first',
                'Cod_setor': 'first',
                'CD_GEOCODB': 'first',
                'NM_BAIRRO': 'first',
                'CD_GEOCODS': 'first',
                'NM_SUBDIST': 'first',
                'CD_GEOCODD': 'first',
                'NM_DISTRIT': 'first',
                'CD_GEOCODM': 'first',
                'NM_MUNICIP': 'first',
                'NM_MICRO': 'first',
                'NM_MESO': 'first',
                'NM_UF': 'first',
                'CD_GEOCODU': 'first',
                'QT_APTOS': 'sum',
                'QT_COMPARECIMENTO': 'sum',
                'QT_ABSTENCOES': 'sum',
                'QT_ELEITORES_BIOMETRIA_NH': 'sum',
                'BRANCO': 'sum',
                'NULO': 'sum'}

    for c in candidates:
        aggr_map[c] = 'sum'

    if aggregate_level == 'Polling place':
        aggr_data = data.groupby('id_polling_place')
    elif aggregate_level == 'Section':
        aggr_data = data.groupby('id_section')
    elif aggregate_level == 'Zone':
        aggr_data = data.groupby('id_zone')
    elif aggregate_level == 'City':
        aggr_data = data.groupby('id_city')
    elif aggregate_level == 'weighting_area':
        aggr_data = data.groupby('Cod_ap')

    data = aggr_data.agg(aggr_map)

    return data


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


def run(region, tse_year, tse_office, tse_turn, tse_aggr, tse_per, candidates, ibge_year, ibge_aggr):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path_tse = data_dir + environ.get('{}_ELECTION_RESULTS'.format(region))
    path_meshblocks = data_dir + environ.get('{}_MESHBLOCKS'.format(region))
    path_matched = data_dir + environ.get('{}_MATCHED_DATA'.format(region))
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
    # Create output office folder
    logger.info('Creating office folder.')
    folder = '{}_{}'.format(tse_office, tse_turn)
    output_filepath = create_folder(output_filepath, folder)
    # Create aggregation folder
    logger.info('Creating aggregation folder.')
    folder = ibge_aggr
    output_filepath = create_folder(output_filepath, folder)
    # Create PER folder
    logger.info('Creating PER folder.')
    folder = tse_per
    output_filepath = create_folder(output_filepath, folder)
    # Create election results folder
    logger.info('Creating election results folder.')
    folder = 'election_results'
    output_filepath = create_folder(output_filepath, folder)

    data = make_spatial_join(input_filepath_tse, input_filepath_meshblocks, output_filepath)
    data = aggregate_data(data, ibge_aggr, candidates)
    data.index = data.index.astype('int64')
    data.to_csv(join(output_filepath, 'data.csv'), index=False)


#run(region='RS',
#    tse_year='2018',
#    tse_office='president',
#    tse_turn='turn_2',
#    tse_aggr='aggr_by_polling_place',
#    tse_per='PER_99.33031',
#    candidates=['FERNANDO HADDAD', 'JAIR BOLSONARO'],
#    ibge_year='2010',
#    ibge_aggr='weighting_area')
