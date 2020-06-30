# -*- coding: utf-8 -*-
import warnings
import pandas as pd
import logging
from os import listdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm

warnings.filterwarnings('ignore')


def process_data(tse_input_filepath, geo_input_filepath, states_output_filepath, final_dataset_output_filepath):
    """ Runs data processing scripts to turn interim election data data from (../interim) into
        processed data with geocoding by polling place (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('1 - Processing data')

    # Loading data with geocode information from polling places
    data_geo = pd.read_csv(geo_input_filepath)

    # Generating an id to facilitate merging
    data_geo['id_polling_place'] = data_geo['SGL_UF'] + data_geo['LOCALIDADE_LOCAL_VOTACAO'] + data_geo['ZONA'].astype(str)  + data_geo['NUM_LOCAL'].astype(str)

    # Listing raw data
    filenames = [filename for filename in listdir(tse_input_filepath) if isfile(join(tse_input_filepath, filename))]

    list_state_df = []
    # For each state...
    for filename in tqdm(filenames):
        # logger.info('Processing {} interim data'.format(filename))

        # Loading raw data
        filepath = tse_input_filepath + filename
        data_tse = pd.read_csv(filepath)

        # Generating an ids to facilitate merging
        data_tse['id_polling_place'] = data_tse['SG_ UF'] + data_tse['NM_MUNICIPIO'] + data_tse['NR_ZONA'].astype(str)  + data_tse['NR_LOCAL_VOTACAO'].astype(str)
        data_tse['id_section'] = data_tse['SG_ UF'] + data_tse['NM_MUNICIPIO'] + data_tse['NR_ZONA'].astype(str)  + data_tse['NR_SECAO'].astype(str)
        data_tse['id_zone'] = data_tse['SG_ UF'] + data_tse['NM_MUNICIPIO'] + data_tse['NR_ZONA'].astype(str)
        data_tse['id_city'] = data_tse['SG_ UF'] + data_tse['NM_MUNICIPIO']

        # Merge datasets
        merged = data_tse.merge(data_geo[['id_polling_place','COD_LOCALIDADE_IBGE','local_unico','lat','lon','geometry','rural','capital','precision', 'lev_dist', 'city_limits']], on='id_polling_place', how='left')

        # Save the data as csv
        merged.to_csv(states_output_filepath + filename, index=False)

        # Append to list
        list_state_df.append(merged)

    # Saving final dataset
    logger.info('2 - Saving final dataset...')
    final_df = pd.concat(list_state_df)
    final_df.to_csv(final_dataset_output_filepath, index=False)

    logger.info('Done!')


if __name__ == '__main__':
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Set data parammeters
    country = 'Brazil'
    election_year = '2018'
    office_folder = 'president'
    turn = '2'
    # Set paths
    tse_input_filepath = project_dir + '/data/interim/{}/TSE/election_data/{}/{}/turn_{}/states/'.format(country,election_year,office_folder,turn)
    geo_input_filepath = project_dir + '/data/processed/{}/TSE/election_data/{}/polling_places/polling_places.csv'.format(country,election_year)
    states_output_filepath = project_dir + '/data/processed/{}/TSE/election_data/{}/{}/turn_{}/states/'.format(country,election_year,office_folder,turn)
    final_dataset_output_filepath = project_dir + '/data/processed/{}/TSE/election_data/{}/{}/turn_{}/data.csv'.format(country,election_year,office_folder,turn)
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Print parameters
    print('======Parameters========')
    print('Country: {}'.format(country))
    print('Election year: {}'.format(election_year))
    print('Office: {}'.format(office_folder))
    print('Turn: {}'.format(turn))

    process_data(tse_input_filepath, geo_input_filepath, states_output_filepath, final_dataset_output_filepath)
