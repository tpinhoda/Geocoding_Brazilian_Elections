# -*- coding: utf-8 -*-
from os import listdir
from os.path import isfile, join
from pathlib import Path

import logging

import pandas as pd


def structure_data(political_office, input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw election data data from (../raw) into
        structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    logger.info('Structuring data...')

    # Listing raw data
    filenames = [filename for filename in listdir(input_filepath) if isfile(join(input_filepath, filename))]


    for filename in filenames:
    
        logger.info('Structuring {} raw data'.format(filename))

        #Loading raw data
        filepath = input_filepath + filename
        raw_data = pd.read_csv(filepath, sep=";", encoding="latin", na_values=["#NULO#", -1, -3])

        #Filtering raw data by political office
        filtered_raw_data = raw_data[(raw_data.DS_CARGO_PERGUNTA == political_office)]

        #Geting the votes per candidates/parties
        votes = filtered_raw_data.copy().set_index(["NM_MUNICIPIO", "NR_ZONA", "NR_SECAO", "NR_LOCAL_VOTACAO", "NM_VOTAVEL"]).unstack(fill_value=0).QT_VOTOS

        #Removing duplicate rows in zones and sections
        filtered_raw_data.drop_duplicates(subset=["NM_MUNICIPIO", "NR_ZONA", "NR_SECAO"], inplace=True)

        #Indexing fitered raw data
        filtered_raw_data.set_index(["NM_MUNICIPIO", "NR_ZONA", "NR_SECAO", "NR_LOCAL_VOTACAO"], inplace=True)

        #Joining votes and filtered dataframes
        structured_data = filtered_raw_data.join(votes)

        #Reset indexes names
        structured_data.reset_index(inplace=True) 
        
        #Save the data as csv
        structured_data.to_csv(output_filepath+filename, index=False)
    
    logger.info('Done!')

    

if __name__ == '__main__':
    #Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    print(project_dir)

    #Set data parammeters
    election_year = '2018'
    political_office = 'Presidente'
    turn = '2'

    #Set paths
    input_filepath = project_dir + '/data/raw/Brazil/election_data/{}/turn_{}/'.format(election_year,turn)
    output_filepath = project_dir + '/data/interim/Brazil/election_data/{}/{}/turn_{}/states/'.format(election_year,political_office,turn)

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    structure_data(political_office, input_filepath, output_filepath)
