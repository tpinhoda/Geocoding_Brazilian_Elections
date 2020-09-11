# Go to mashmaper.com ====================

# -*- coding: utf-8 -*-
import warnings
import logging
import geopandas as gpd
from os import environ, mkdir
from os.path import join
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def dissolve_data(input_path, output_path, region, aggr, buffer=0):
    logger = logging.getLogger(__name__)
    logger.info('Dissolving meshblock according to: {}'.format(aggr))
    if aggr == 'sub_dist':
        aggr_attr = 'CD_GEOCODS'
    elif aggr == 'census_tract':
        aggr_attr = 'CD_GEOCODI'
    elif aggr == 'dist':
        aggr_attr = 'CD_GEOCODD'
    elif aggr == 'municipality':
        aggr_attr = 'CD_GEOCODM'
    elif aggr == 'weighting_area':
        aggr_attr = 'Cod_ap'
    # Read meshblock
    data = gpd.read_file(join(input_path, region+'.shp'))
    # data['geometry'] = data.buffer(buffer)
    # Aggregate the data according to aggr_attr
    data = data.dissolve(by=aggr_attr, aggfunc='first', as_index=False)
    # Get only the boundary coordinates for each polygon
    #data['geometry'] = [Polygon(poly) for poly in data.exterior]
    # Generate a folder for the aggr file
    exist, out_path = create_folder(output_path, aggr)
    # data.to_file(join(out_path, region + '.gpkg'), layer=aggr, driver="GPKG")
    data.to_file(join(out_path, 'shapefiles', region + '.shp'))


def create_folder(path, folder_name):
    logger = logging.getLogger(__name__)
    path = join(path, folder_name)
    exist = 0
    try:
        mkdir(path)
    except FileExistsError:
        logger.info('Folder already exist.')
        exist = 1
    return exist, path


def run(region, year, aggr, buffer=0):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path = data_dir + environ.get('MESHBLOCKS')
    # Generate input output paths
    interim_path = path.format(region, year, 'interim')
    processed_path = path.format(region, year, 'processed')
    # Set paths
    input_filepath = join(processed_path, 'census_tract', 'shapefiles')
    output_filepath = processed_path
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    # print('======Parameters========')
    # print('Census year: {}'.format(year))

    dissolve_data(input_filepath, output_filepath, region, aggr)

