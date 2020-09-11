# -*- coding: utf-8 -*-
import warnings
import logging
import json
import pandas as pd
from os import environ
from os.path import join
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def join_data(input_path, output_path, categories_path):
    """ Join census data by categorie from (../processed) (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    # Get the json with the file names
    with open(join(categories_path, 'categories.json'), 'r') as f:
        categories = json.load(f)

    for category in categories:
        list_df = []
        logger.info('Joining files: ' + category)
        for filename in tqdm(categories[category]):
            data = pd.read_csv(join(input_path, filename))
            code_cols = data.columns.values[0:20]

            if len(list_df):
                data.drop(code_cols, axis=1, inplace=True)
                cols = [filename.split('.')[0] + '_' + c for c in data.columns.values]
                data.columns = cols
            else:
                cols = [filename.split('.')[0] + '_' + c for c in data.columns.values[20:len(data.columns)]]
                data.columns = code_cols.tolist() + cols
            list_df.append(data)
        concat_data = pd.concat(list_df, axis=1)
        if category != 'Tudo':
            final_output_path = join(output_path, 'joined_by_category')
            concat_data.to_csv(join(final_output_path, category + '.csv'), index=False)
        else:
            final_output_path = join(output_path, 'joined_all')
            concat_data.to_csv(join(final_output_path, 'data.csv'), index=False)


def run(region, year, aggr):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get census results path
    path = data_dir + environ.get('CENSUS_DATA')
    # Generate input output paths
    processed_path = path.format(region, year, 'processed')
    external_path = path.format(region, year, 'external')
    # Set paths
    input_filepath = join(processed_path, aggr, 'not_joined')
    output_filepath = join(processed_path, aggr)
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print('======Parameters========')
    print('Census year: {}'.format(year))

    join_data(input_filepath, output_filepath, external_path)
