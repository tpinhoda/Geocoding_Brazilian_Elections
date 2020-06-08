# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')

from os import listdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm

import logging

import pandas as pd


def structure_data(political_office, input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw election data data from (../raw) into
        structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    # Listing raw data
    filenames = [filename for filename in listdir(input_filepath) if isfile(join(input_filepath, filename))]

    logger.info('Structuring raw data')
    for filename in tqdm(filenames, leave=False):
    
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

        #Drop uncessary collumns
        unecessary_cols = ['NR_PARTIDO', 'SG_PARTIDO', 'NM_PARTIDO', 'CD_TIPO_VOTAVEL', 'DS_TIPO_VOTAVEL', 'NR_VOTAVEL', 'NM_VOTAVEL', 'QT_VOTOS']
        structured_data.drop(labels=unecessary_cols, axis = 1, inplace=True)

        #Reset indexes names
        structured_data.reset_index(inplace=True) 
        
        #Save the data as csv
        structured_data.to_csv(output_filepath+filename, index=False)
    
    logger.info('Done!')

    

if __name__ == '__main__':
    #Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    print(project_dir)

    #Set data parammeters
    country = 'Brazil'
    election_year = '2018'
    political_office = 'Presidente'
    office_folder = 'president'
    turn = '2'



    #Set paths
    input_filepath = project_dir + '/data/raw/{}/election_data/{}/turn_{}/'.format(country, election_year, turn)
    output_filepath = project_dir + '/data/interim/{}/election_data/{}/{}/turn_{}/states/'.format(country, election_year, office_folder,turn)

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    #Print parameters
    print('======Parameters========')
    print('Country: {}'.format(country))
    print('Election year: {}'.format(election_year))
    print('Office: {}'.format(political_office))
    print('Turn: {}'.format(turn))
    


    structure_data(political_office, input_filepath, output_filepath)
