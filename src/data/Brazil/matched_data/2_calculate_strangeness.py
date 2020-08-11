# -*- coding: utf-8 -*-
import warnings
import logging
import geopandas as gpd
import pandas as pd
from os import listdir, environ, mkdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm
from shapely.geometry import Point
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def run(region, tse_year, tse_office, tse_turn, tse_aggr, tse_per, candidates, ibge_year, ibge_aggr):
    # Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get census results path
    path_tse = project_dir + environ.get('{}_ELECTION_RESULTS'.format(region))
    path_meshblocks = project_dir + environ.get('{}_MESHBLOCKS'.format(region))
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

  


run(region='RS',
    tse_year='2018',
    tse_office='president',
    tse_turn='turn_2',
    tse_aggr='aggr_by_polling_place',
    tse_per='PER_99.33031',
    candidates=['FERNANDO HADDAD', 'JAIR BOLSONARO'],
    ibge_year='2010',
    ibge_aggr='weighting_area')
