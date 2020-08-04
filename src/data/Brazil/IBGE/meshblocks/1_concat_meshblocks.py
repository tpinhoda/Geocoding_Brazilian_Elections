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


def concat_data(input_path, output_path, region):
    folders = [folder for folder in listdir(input_path)]
    files = dict()
    for folder in folders:
        files[folder] = [filename for filename in listdir(join(input_path, folder)) if filename.endswith(".shp")][0]

    list_gdf = []
    for folder in files:
        data = gpd.read_file(join(input_path, folder, files[folder]))
        data['NM_UF'] = [folder] * len(data)
        data['CD_GEOCODU'] = [files[folder][0:2]] * len(data)

        try:
            data.drop('ID', axis=1, inplace=True)
            data.drop('ID1', axis=1, inplace=True)
        except KeyError:
            donothing = 'hue'

        list_gdf.append(data)

    concat_df = gpd.GeoDataFrame(pd.concat(list_gdf, axis=0), crs={'init': 'GRS:1980'})
    concat_df.to_file(join(output_path, region + '.gpkg'), layer='census_tract', driver="GPKG")


def run(region, year):
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get census results path
    path = project_dir + environ.get('{}_MESHBLOCKS'.format(region))
    # Generate input output paths
    raw_path = path.format(year, 'raw')
    interim_path = path.format(year, 'interim')
    # Set paths
    input_filepath = raw_path
    output_filepath = interim_path
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print('======Parameters========')
    print('Census year: {}'.format(year))

    concat_data(input_filepath, output_filepath, region)

