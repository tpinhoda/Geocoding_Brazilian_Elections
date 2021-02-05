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


def add_sufix_to_columns(data, code_cols, census_cols, global_cols):
    code_cols = {c: 'GEO_'+c for c in code_cols}
    global_cols = {c: 'GLOBAL_' + c for c in global_cols}
    census_cols = {c: 'CENSUS_' + c for c in census_cols}
    data.rename(columns=code_cols, inplace=True)
    data.rename(columns=global_cols, inplace=True)
    data.rename(columns=census_cols, inplace=True)
    return data


def join_data(input_path, output_path, categories_path):
    """ Join census data by categorie from (../processed) (saved in ../processed).
    """
    code_cols = ['Cod_Setor', 'Cod_ap', 'Cod_Grande_Regiao', 'Nome_Grande_Regiao', 'Cod_UF',
                 'Nome_UF', 'Cod_Meso', 'Nome_Meso', 'Cod_Micro', 'Nome_Micro',
                 'Cod_RM', 'Nome_RM', 'Cod_Municipio', 'Nome_Municipio',
                 'Cod_Distrito', 'Nome_Distrito', 'Cod_Subdistrito',
                 'Nome_Subdistrito', 'Cod_Bairro', 'Nome_Bairro']

    logger = logging.getLogger(__name__)
    # Get the json with the file names
    with open(join(categories_path, 'categories.json'), 'r') as f:
        categories = json.load(f)

    for category in categories:
        list_df = []
        logger.info('Joining files: ' + category)
        for filename in tqdm(categories[category]):
            data = pd.read_csv(join(input_path, filename))
            # Organize columns
            var_cols = [c for c in data.columns if c not in code_cols]
            data = data[code_cols + var_cols]
            # Concat
            if len(list_df):
                data.drop(code_cols, axis=1, inplace=True)
                cols = [filename.split('.')[0] + '_' + c for c in data.columns.values]
                data.columns = cols
            else:
                cols = [filename.split('.')[0] + '_' + c for c in data.columns if c not in code_cols]
                data.columns = code_cols + cols
            list_df.append(data)
        concat_data = pd.concat(list_df, axis=1)
        if category != 'Tudo':
            final_output_path = join(output_path, 'joined_by_category')
            concat_data.to_csv(join(final_output_path, category + '.csv'), index=False)
        else:
            # Create Global variables
            concat_data['population'] = concat_data['Pessoa03_V001']
            # Normalize Data
            basic_cols = [c for c in concat_data if 'Basico' in c]
            global_cols = ['population']
            census_cols = [c for c in concat_data.columns if c not in code_cols + basic_cols + global_cols]

            # Normalize Basic columns by min max
            concat_data[basic_cols] = (concat_data[basic_cols] - concat_data[basic_cols].min()) / (
                    concat_data[basic_cols].max() - concat_data[basic_cols].min())
            # Normaize Census columns by population size
            concat_data[census_cols] = concat_data[census_cols].divide(concat_data['Pessoa03_V001'], axis=0)
            # Adding suffix names to columns
            concat_data = add_sufix_to_columns(concat_data, code_cols, basic_cols + census_cols, global_cols)
            # Replace NANs by 0
            concat_data.fillna(0, inplace=True)
            # Remove duplicated columns
            census_cols = [c for c in concat_data.columns if 'CENSUS' in c]
            global_cols = [c for c in concat_data.columns if 'GLOBAL' in c]
            geo_cols = [c for c in concat_data.columns if 'GEO' in c]
            no_duplicates = concat_data[census_cols].T.drop_duplicates().T

            cols = geo_cols + global_cols + no_duplicates.columns.values.tolist()
            concat_data = concat_data[cols].copy()
            final_output_path = join(output_path, 'joined_all')
            print(concat_data)
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
