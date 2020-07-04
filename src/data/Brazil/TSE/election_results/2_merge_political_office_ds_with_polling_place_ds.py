# -*- coding: utf-8 -*-
import warnings
import pandas as pd
import logging
from os import listdir, environ
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def merge_data(election_results, polling_places, states_output_filepath, final_dataset_output_filepath):
    """ Runs scripts to turn merge interim election data data from (../interim/election_results) with
        polling places processed data from (../processed/polling_places).

        The results of this script is saved in ../interim/election_results
    """
    logger = logging.getLogger(__name__)
    logger.info('1 - Merging data')
    # Loading data with geocode information from polling places
    polling_places = pd.read_csv(polling_places)
    # Generating an id to facilitate merging
    polling_places['id_polling_place'] = polling_places['SGL_UF'] + polling_places['LOCALIDADE_LOCAL_VOTACAO'] + \
                                         polling_places['ZONA'].astype(str) + polling_places['NUM_LOCAL'].astype(str)

    # Listing raw data
    filenames = [filename for filename in listdir(election_results) if isfile(join(election_results, filename))]
    list_state_df = []
    # For each state...
    for filename in tqdm(filenames):
        # Loading raw data
        filepath = election_results + filename
        results = pd.read_csv(filepath)
        # Converting columns to string type
        results['NR_ZONA'] = results['NR_ZONA'].astype(str)
        results['NR_LOCAL_VOTACAO'] = results['NR_LOCAL_VOTACAO'].astype(str)
        results['NR_SECAO'] = results['NR_SECAO'].astype(str)
        # Generating an ids to facilitate merging
        id_pl = results['SG_ UF'] + results['NM_MUNICIPIO'] + results['NR_ZONA'] + results['NR_LOCAL_VOTACAO']
        id_sec = results['SG_ UF'] + results['NM_MUNICIPIO'] + results['NR_ZONA'] + results['NR_SECAO']
        id_zone = results['SG_ UF'] + results['NM_MUNICIPIO'] + results['NR_ZONA']
        id_city = results['SG_ UF'] + results['NM_MUNICIPIO']
        # Imputing columns ids
        results['id_polling_place'] = id_pl
        results['id_section'] = id_sec
        results['id_zone'] = id_zone
        results['id_city'] = id_city
        # Merge datasets
        merged = results.merge(polling_places[
                                   ['id_polling_place', 'COD_LOCALIDADE_IBGE', 'local_unico', 'lat', 'lon', 'geometry',
                                    'rural', 'capital', 'precision', 'lev_dist', 'city_limits']],
                               on='id_polling_place', how='left')
        # Save the data as csv
        merged.to_csv(states_output_filepath + filename, index=False)

        # Append to list
        list_state_df.append(merged)

    # Saving final dataset
    logger.info('2 - Saving final dataset...')
    final_df = pd.concat(list_state_df)
    final_df.to_csv(final_dataset_output_filepath + '/Brazil.csv', index=False)

    logger.info('Done!')


if __name__ == '__main__':
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get election results path
    election_results_path = project_dir + environ.get('BRAZIL_ELECTION_RESULTS')
    polling_places_path = project_dir + environ.get('BRAZIL_POLLING_PLACES')
    # Set data parameters
    year = '2018'
    office_folder = 'president'
    turn = '2'
    # Generate input output paths
    election_results_interim_path = election_results_path.format(year, 'interim')
    polling_places_processed_path = polling_places_path.format(year, 'processed')
    # Set paths
    election_results_path = election_results_interim_path + '/{}/turn_{}/'.format(office_folder,
                                                                                  turn)
    polling_places_path = polling_places_processed_path + '/polling_places.csv'
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print('======Parameters========')
    print('Election year: {}'.format(year))
    print('Office: {}'.format(office_folder))
    print('Turn: {}'.format(turn))

    merge_data(election_results_path, polling_places_path, election_results_path, election_results_path)
