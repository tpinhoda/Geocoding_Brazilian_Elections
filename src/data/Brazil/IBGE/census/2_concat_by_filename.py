# -*- coding: utf-8 -*-
import warnings
import logging
import json
import pandas as pd
from os import listdir, environ
from os.path import join
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def concat_data(input_path, output_path, filenames_path):
    """ Concatenate census data from (../interim) (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    # Get the json with the file names
    with open(join(filenames_path, 'categories.json'), 'r') as f:
        categories = json.load(f)
    # Get the states folders
    folders = [folder for folder in listdir(input_path)]
    for filename in categories['Tudo']:
        df_list = []
        logger.info('Concatenating files: ' + filename)
        for folder in tqdm(folders):
            state_path = join(input_path, folder)
            data = pd.read_csv(join(state_path, filename), encoding='utf-8')
            df_list.append(data)
        concat_df = pd.concat(df_list, axis=0)
        # Save the concatenated data
        final_output_path = join(output_path, 'not_joined')
        concat_df.to_csv(join(final_output_path, filename), index=False)
    logger.info('Done!')


def run(region, year, aggr):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path = data_dir + environ.get('{}_CENSUS_DATA'.format(region))
    # Generate input output paths
    interim_path = path.format(year, 'interim')
    processed_path = path.format(year, 'processed')
    external_path = path.format(year, 'external')
    # Set paths
    input_filepath = interim_path
    output_filepath = join(processed_path, aggr)
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print('======Parameters========')
    print('Census year: {}'.format(year))

    concat_data(input_filepath, output_filepath, external_path)
