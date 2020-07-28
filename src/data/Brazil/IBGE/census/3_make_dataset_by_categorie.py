# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
import json
from pathlib import Path
from tqdm import tqdm

warnings.filterwarnings('ignore')


def read_json(path):
    with open(path) as json_file:
        data = json.load(json_file)
    return data


def join_data(input_filepath, categories_filepath, output_filepath):
    """ Runs data processing scripts to turn raw census data data from (../raw) into
        structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)

    # Reading reference file
    categories = read_json(categories_filepath)
    del categories['Tudo']
    del categories['Registro_Civil']
    for type, filenames in categories.items():
        list_data = []
        id_data = 1
        for filename in filenames:
            data = pd.read_csv(input_filepath + filename)
            columns = data.columns.values.tolist()
            columns.remove('Cod_ap')
            columns.remove('CD_GEOCODM')
            columns.remove('NM_MUNICIP')
            columns = ['0' + str(id_data) + '_' + col_name for col_name in columns]
            columns = ['Cod_ap', 'CD_GEOCODM', 'NM_MUNICIP'] + columns
            data.columns = columns
            list_data.append(data)
            id_data = id_data + 1

        data = pd.concat(list_data, axis=1, sort=False)
        data = data.loc[:, ~data.columns.duplicated()]
        columns = data.columns.values
        constant_columns = (data != data.iloc[0]).any() == False
        to_drop = columns[constant_columns].tolist()
        data.drop(to_drop, axis=1, inplace=True)
        data.to_csv(output_filepath + type + '.csv', index=False)


if __name__ == '__main__':
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    print(project_dir)

    # Set data parammeters
    country = 'Brazil'
    census_year = '2010'
    state = 'RS'
    # Set paths
    input_filepath = project_dir + '/data/ronaldo/data/IBGE/census_2010/RS/aggregated_by_weighting_area/not_joined/'
    output_filepath = project_dir + '/data/ronaldo/data/IBGE/census_2010/RS/aggregated_by_weighting_area/joined/'
    categories_filepath = project_dir + '/data/ronaldo/data/IBGE/census_2010/categories.json'
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Print parameters
    print('======Parameters========')
    print('Country: {}'.format(country))
    print('Census year: {}'.format(census_year))
    print('State: {}'.format(state))

    join_data(input_filepath, categories_filepath, output_filepath)
