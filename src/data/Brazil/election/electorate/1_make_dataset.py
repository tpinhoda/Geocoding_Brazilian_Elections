# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')

from os import listdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm

import logging

import pandas as pd
import numpy as np


def make_profile_dataset(input_filepath,output_filepath):
    logger = logging.getLogger(__name__)

    columns_pre_2018 = ['PERIODO', 'UF',
                        'MUNICIPIO', 'CD_MUNICIPIO', 'NR_ZONA', 'DS_GENERO', 'DS_FAIXA_ETARIA', 'DS_GRAU_ESCOLARIDADE', 'QT_ELEITORES_PERFIL']
    columns_2018 = ['DT_GERACAO', 'HH_GERACAO', 'ANO_ELEICAO', 'SG_UF', 'CD_MUNICIPIO',
                    'NM_MUNICIPIO', 'CD_MUN_SIT_BIOMETRIA', 'DS_MUN_SIT_BIOMETRIA',
                    'NR_ZONA', 'CD_GENERO', 'DS_GENERO', 'CD_ESTADO_CIVIL',
                    'DS_ESTADO_CIVIL', 'CD_FAIXA_ETARIA', 'DS_FAIXA_ETARIA',
                    'CD_GRAU_ESCOLARIDADE', 'DS_GRAU_ESCOLARIDADE', 'QT_ELEITORES_PERFIL',
                    'QT_ELEITORES_BIOMETRIA', 'QT_ELEITORES_DEFICIENCIA',
                    'QT_ELEITORES_INC_NM_SOCIAL']
    columns_to_use = ['CD_MUNICIPIO', 'DS_GENERO',
                      'DS_FAIXA_ETARIA', 'DS_GRAU_ESCOLARIDADE', 'QT_ELEITORES_PERFIL']
    metadata_by_year = {
        2018: {
            'file_extension': 'csv',
            'columns': columns_2018,
            'header': 0
        }
    }

    filenames = [filename for filename in listdir(input_filepath) if isfile(join(input_filepath, filename))]

    logger.info('Structuring raw data')
    for filename in tqdm(filenames, leave=False):
    #    logger.info('starting to create {} profile data'.format(2018))
        
        filepath = input_filepath + filename
        dataset = pd.read_csv(filepath,
                           encoding='latin',
                           sep=';', names=metadata_by_year[2018]['columns'],
                           usecols=columns_to_use,
                           header=metadata_by_year[2018]['header'],
                           na_values='INFORMAÇÃO NÃO RECUPERADA')

        object_df = dataset.select_dtypes(include='object')
        dataset.loc[:, object_df.columns] = object_df.apply(
            lambda column: column.str.strip().str.replace(' ', '_').str.upper())

        dataset = get_dummies(dataset).groupby(['SG_UF','CD_MUNICIPIO','NR_ZONA', 'NR_SECAO']).apply(lambda group: group.apply(lambda column: (
            column * group.QT_ELEITORES_PERFIL).sum() if column.name != 'QT_ELEITORES_PERFIL' else column.sum()))
        #dataset = dataset.drop(columns='CD_MUNICIPIO')
        #dataset = dataset.loc[dataset.index.intersection(correspondence.index)]
        #dataset.index.name = 'CD_MUNICIPIO'
        dataset.reset_index()

        dataset.to_csv(output_filepath)
   #     logger.info('done creating {} profile data'.format(2018))

    

if __name__ == '__main__':
    #Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    print(project_dir)

    #Set data parammeters
    country = 'Brazil'
    election_year = '2018'

    #Set paths
    input_filepath = project_dir + '/data/raw/{}/election_data/{}/electorate_profile/'.format(country, election_year)
    output_filepath = project_dir + '/data/interim/{}/election_data/{}/electorate_profile/states/'.format(country, election_year)

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    #Print parameters
    print('======Parameters========')
    print('Country: {}'.format(country))
    print('Election year: {}'.format(election_year))
    

    make_profile_dataset(input_filepath, output_filepath)
