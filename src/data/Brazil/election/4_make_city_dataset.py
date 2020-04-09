# -*- coding: utf-8 -*-

# To run this script the 1_make_political_office_dataset.py should be already executed

from os import listdir
from os.path import isfile, join
from pathlib import Path

import logging

import pandas as pd


def filter_data(political_office, input_filepath, output_filepath, cities):
    """ Runs data processing scripts to turn raw election data data from (../raw) into
        structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    logger.info('Structuring data...')

    # Listing raw data
    

    for city in cities:

        logger.info('Filtering {} ({}) data'.format(city[0], city[1]))

        #Loading raw data
        filepath = input_filepath + city[1] + '.csv'
        data = pd.read_csv(filepath)

        #Filtering raw data by political office
        filtered_data = data[(data.NM_MUNICIPIO == city[0].upper())]

        #Save the data as csv
        filtered_data.to_csv(output_filepath+city[0]+'.csv', index=False)
    
    logger.info('Done!')

    

if __name__ == '__main__':
    #Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    print(project_dir)

    #Set data parammeters
    election_year = '2018'
    political_office = 'Presidente'
    turn = '2'

    #cities = [('Rio Branco', 'AC'), ('Macapá', 'AP'), ('Manaus', 'AM'), ('Belém', 'PA'), ('Porto Velho', 'RO'), ('Boa Vista', 'RR'), 
    #          ('Palmas', 'TO'), ('Maceió', 'AL'), ('Salvador', 'BA'), ('Fortaleza', 'CE'), ('São Luís', 'MA'), ('João Pessoa', 'PB'), 
    #          ('Recife', 'PE'), ('Teresina', 'PI'), ('Natal', 'RN'), ('Aracaju',  'SE'), ('Goiânia', 'GO'), ('Cuiabá', 'MT'),
    #          ('Campo Grande', 'MS'), ('Brasília', 'DF'), ('Vitória', 'ES'), ('Belo Horizonte', 'MG'), ('São Paulo', 'SP'), 
    #          ('Rio de Janeiro', 'RJ'), ('Curitiba', 'PR'), ('Porto Alegre', 'RS'), ('Florianópolis', 'SC')]

    cities = [('São Carlos', 'SP'), ('Bauru', 'SP'), ('Ribeirão Preto', 'SP')]          
    #Set paths
    input_filepath = project_dir + '/data/interim/Brazil/election_data/{}/{}/turn_{}/states/'.format(election_year,political_office,turn)
    output_filepath = project_dir + '/data/interim/Brazil/election_data/{}/{}/turn_{}/cities/non_capitals/'.format(election_year,political_office,turn)

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    filter_data(political_office, input_filepath, output_filepath, cities)
