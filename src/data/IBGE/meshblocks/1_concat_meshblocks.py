# -*- coding: utf-8 -*-
import warnings
import logging
import geopandas as gpd
import pandas as pd
from os import listdir, environ
from os.path import join
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def concat_data(input_path):
    logger = logging.getLogger(__name__)
    logger.info('Concatenating meshblocks.')
    # Get the folders with shapefiles
    folders = [folder for folder in listdir(input_path)]
    # Create a dictionary where the .shp will be saved
    files = dict()
    # Get the .shp files in each folder
    for folder in folders:
        files[folder] = [filename for filename in listdir(join(input_path, folder)) if filename.endswith(".shp")][0]
    # Create a list to save the GeoDataframes
    list_gdf = []
    for folder in tqdm(files, leave=True):
        # Read the meshblock
        data = gpd.read_file(join(input_path, folder, files[folder]))
        # Generate the state abbreviation columns
        data['NM_UF'] = [folder] * len(data)
        data['CD_GEOCODU'] = [files[folder][0:2]] * len(data)
        # Drop unnecessary columns
        try:
            data.drop('ID', axis=1, inplace=True)
            data.drop('ID1', axis=1, inplace=True)
        except KeyError:
            donothing = 'hue'
        # Append the GeoDataFrame
        list_gdf.append(data)
    # Check the number of GeoDataframes loaded
    if len(list_gdf) > 1:
        # Generate a single GeoDataframe
        concat_df = gpd.GeoDataFrame(pd.concat(list_gdf, axis=0), crs='GRS:1980')
        return concat_df
    else:
        data.crs = "EPSG:4326"
        return data


def add_weighting_area_code(meshblock, reference_filepath):
    logger = logging.getLogger(__name__)
    logger.info('Associating weighting area code.')
    # Reading reference file
    reference = pd.read_csv(reference_filepath)
    # Converting census tract code to integer
    reference['Cod_setor'] = reference['Cod_setor'].astype('int64')
    # Renaming the census tract code in the meshblock data
    meshblock.rename(columns={'CD_GEOCODI': 'Cod_setor'}, inplace=True)
    # Converting census tract code to integer
    meshblock['Cod_setor'] = meshblock['Cod_setor'].astype('int64')
    # Merging reference and meshblock to get Cod_ap
    meshblock = meshblock.merge(reference, on='Cod_setor', how='left')
    # Dropping census tract without Cod_ap
    meshblock.dropna(subset=['Cod_ap'], inplace=True)
    # Converting weighting area codes to integer
    meshblock['Cod_ap'] = meshblock['Cod_ap'].astype('int64')
    return meshblock


def run(region, year):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path = data_dir + environ.get('MESHBLOCKS')
    # Generate input output paths
    raw_path = path.format(region, year, 'raw')
    processed_path = path.format(region, year, 'processed')
    external_path = path.format(region, year, 'external')
    # Set paths
    input_filepath = raw_path
    output_filepath = join(processed_path, 'census_tract')
    reference_filepath = join(external_path, 'census_tract_x_weighting_area.csv')
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    # print('======Parameters========')
    # print('Census year: {}'.format(year))

    meshblock = concat_data(input_filepath)
    meshblock = add_weighting_area_code(meshblock, reference_filepath)
    meshblock.to_file(join(output_filepath, 'shapefiles', region + '.shp'))
