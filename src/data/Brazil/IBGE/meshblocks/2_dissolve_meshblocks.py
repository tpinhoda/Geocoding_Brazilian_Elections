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


def dissolve_data(input_path, output_path, region, aggr, buffer):
    if aggr == 'sub_dist':
        aggr_attr = 'CD_GEOCODS'
    elif aggr == 'census_tract':
        aggr_attr = 'CD_GEOCODI'
    elif aggr == 'dist':
        aggr_attr = 'CD_GEOCODD'
    elif aggr == 'municipality':
        aggr_attr = 'CD_GEOCODM'

    data = gpd.read_file(join(input_path, region+'.gpkg'))
    data['geometry'] = data.buffer(buffer)
    data = data.dissolve(by=aggr_attr, aggfunc='first')
    exist, out_path = create_folder(output_path, aggr)
    data.to_file(join(out_path, region + '.gpkg'), layer=aggr, driver="GPKG")


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


def run(region, year, aggr, buffer):
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get census results path
    path = project_dir + environ.get('{}_MESHBLOCKS'.format(region))
    # Generate input output paths
    interim_path = path.format(year, 'interim')
    processed_path = path.format(year, 'processed')
    # Set paths
    input_filepath = interim_path
    output_filepath = processed_path
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print('======Parameters========')
    print('Census year: {}'.format(year))

    dissolve_data(input_filepath, output_filepath, region, aggr, buffer)


run('Brazil', '2010', 'sub_dist', 0.00001)
