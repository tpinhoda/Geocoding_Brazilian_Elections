# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import listdir, environ, remove
from os.path import isfile, join
from tqdm import tqdm
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def structure_data(office, input_path, output_path):
    """ Runs data processing scripts to turn raw election data data from (../raw) into
        structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    # List raw data
    filenames = [filename for filename in listdir(input_path) if isfile(join(input_path, filename))]
    # Structure each file in filenames
    logger.info('Structuring data from:\n' + input_path)
    for filename in tqdm(filenames, leave=False):
        # Load raw data
        filepath = input_path + filename
        raw_data = pd.read_csv(filepath, sep=";", encoding="latin1", na_values=["#NULO#", -1, -3])
        raw_data.DS_CARGO_PERGUNTA = raw_data.DS_CARGO_PERGUNTA.str.upper()
        raw_data.NM_VOTAVEL = raw_data.NM_VOTAVEL.str.upper()
        # raw_data['NM_VOTAVEL'] = raw_data['NM_VOTAVEL'].str.replace('[^A-Za-z\s]+', '')
        # Filter raw data by political office
        filtered_raw_data = raw_data[(raw_data.DS_CARGO_PERGUNTA == office.upper())]
        # Get the votes per candidates/parties
        votes = filtered_raw_data.copy().set_index(["NM_MUNICIPIO",
                                                    "NR_ZONA",
                                                    "NR_SECAO",
                                                    "NR_LOCAL_VOTACAO",
                                                    "NM_VOTAVEL"]).unstack(fill_value=0).QT_VOTOS
        # Remove duplicate rows in zones and sections
        filtered_raw_data.drop_duplicates(subset=["NM_MUNICIPIO", "NR_ZONA", "NR_SECAO"], inplace=True)
        # Index filtered raw data
        filtered_raw_data.set_index(["NM_MUNICIPIO", "NR_ZONA", "NR_SECAO", "NR_LOCAL_VOTACAO"], inplace=True)
        # Join votes and filtered dataframes
        structured_data = filtered_raw_data.join(votes)
        # Drop unnecessary columns
        unnecessary_cols = ['NR_PARTIDO',
                            'SG_PARTIDO',
                            #'NM_PARTIDO',
                            'CD_TIPO_VOTAVEL',
                            #'DS_TIPO_VOTAVEL',
                            'NR_VOTAVEL',
                            'NM_VOTAVEL',
                            'QT_VOTOS']
        structured_data.drop(labels=unnecessary_cols, axis=1, inplace=True)
        # Reset indexes names
        structured_data.reset_index(inplace=True)
        # Save the data as csv
        structured_data.to_csv(output_path + filename, index=False)
    logger.info('Data saved in:\n' + output_path)
    logger.info('Done!')


def run(region, year, political_office, office_folder, turn):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get election results path
    path = join(data_dir, environ.get('{}_ELECTION_RESULTS'.format(region)))
    # Generate input output paths
    raw_path = path.format(year, 'raw')
    interim_path = path.format(year, 'interim')
    # Set paths
    input_filepath = raw_path + '/turn_{}/'.format(turn)
    output_filepath = interim_path + '/{}/turn_{}/'.format(office_folder, turn)
    # Remove existing files
    file_list = [f for f in listdir(output_filepath)]
    for f in file_list:
        remove(join(output_filepath, f))
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Print parameters
    print('======Parameters========')
    print('Election year: {}'.format(year))
    print('Office: {}'.format(political_office))
    print('Turn: {}'.format(turn))

    structure_data(political_office, input_filepath, output_filepath)
