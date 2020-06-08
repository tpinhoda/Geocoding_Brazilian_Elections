# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')

from os import listdir
from os.path import isfile, join
from pathlib import Path
from scipy import stats

from tqdm import tqdm
from scipy import stats

import pingouin as pg
import numpy as np
import pandas as pd
import logging
import seaborn as sns
import matplotlib.pyplot as plt

def process_data(tse_input_filepath, output_filepath):

    """ Runs data processing scripts to turn interim election data from (../interim) into
        processed data with integrity measures features (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('1 - Generating outliers marks per city for each state')

    # Listing raw data
    filenames = [filename for filename in listdir(tse_input_filepath) if isfile(join(tse_input_filepath, filename))]

    # DF need to be removed because it is only one city
    #filenames.remove('DF.csv')
    # For each state...
    for filename in tqdm(filenames):
        #logger.info('Processing {} interim data'.format(filename))

        #Loading raw data
        filepath = tse_input_filepath + filename
        data_tse = pd.read_csv(filepath)

        # Generating an id to facilitate grouping
        data_tse['id'] = data_tse['NM_MUNICIPIO'] + data_tse['NR_ZONA'].astype(str)  + data_tse['NR_LOCAL_VOTACAO'].astype(str)

        # Generate capital marks
        #data_tse = generate_capitals_mark(data_tse)
       
        data_tse = generate_kruskal_wallis_statistics(data_tse, 'FERNANDO HADDAD')
        data_tse = generate_kruskal_wallis_statistics(data_tse, 'JAIR BOLSONARO')
        data_tse = generate_kruskal_wallis_statistics(data_tse, 'Nulo')
        data_tse = generate_kruskal_wallis_statistics(data_tse, 'Branco')



        # Removing capitals
       # data_tse = data_tse[data_tse['capital'] == False]

        # Generate measure of integrity features
        #data_tse = generate_outlier_cities_marks(data_tse, filename ,critical_value=2.575)

        # If Capital is excluded
        #data_tse.drop('capital', axis=1, inplace=True)
        
        # Save the data as csv
        data_tse.to_csv(output_filepath+filename, index=False)


    logger.info('1.1 - Done!')

def generate_percentages_votes(data, candidates):
    for candidate in candidates:
        data[candidate+'_%'] = 100*data[candidate]/data['QT_COMPARECIMENTO']
    return data     

def generate_capitals_mark(df_polling_places):

    capitals = { 'AC': 'RIO BRANCO',
                 'AP': 'MACAPÁ',
                 'AM': 'MANAUS',
                 'PA': 'Belém',
                 'RO': 'PORTO VELHO',
                 'RR': 'BOA VISTA',
                 'TO': 'PALMAS',
                 'AL': 'MACEIÓ',
                 'BA': 'SALVADOR',
                 'CE': 'FORTALEZA',
                 'MA': 'SÃO LUÍS',
                 'PB': 'JOÃO PESSOA',
                 'PE': 'RECIFE',
                 'PI': 'TERESINA',
                 'RN': 'NATAL',
                 'SE': 'ARACAJU',
                 'GO': 'GOIÂNIA',
                 'MT': 'CUIABÁ',
                 'MS': 'CAMPO GRANDE',
                 'DF': 'BRASÍLIA',
                 'ES': 'VITÓRIA',
                 'MG': 'BELO HORIZONTE',
                 'SP': 'SÃO PAULO',
                 'RJ': 'RIO DE JANEIRO',
                 'PR': 'CURITIBA',
                 'RS': 'PORTO ALEGRE',
                 'SC': 'FLORINÓPOLIS'
    }
    capitals_l= []
    for index, row in df_polling_places.iterrows():
        if row['NM_MUNICIPIO'] == capitals[row['SG_ UF']]:
            capitals_l.append(True)
        else:
            capitals_l.append(False)

    df_polling_places['capital'] = capitals_l
    return df_polling_places



def generate_quantile_cities_marks(data, filename, critical_value=2.575, field='QT_COMPARECIMENTO'):
    # Grouping by city
    city_data = data.groupby('CD_MUNICIPIO', as_index = False).agg('sum')[['CD_MUNICIPIO',field]]
    # Transform sum of electorate
    city_data['log_'+field] = city_data[field].transform(np.log)    
    # Sort vales by electorate
    city_data.sort_values(by=field, inplace=True)

    # Calculating first and third quartiles
    Q1, Q3 = np.percentile(city_data[field], [25,75])
 
    # Getting cities in the first quartile
    city_data['less_Q1'] = city_data[field] < Q1
    sum(city_data['less_Q1'])
    # Associating cities to outliers marks
    data = data.merge(city_data[['CD_MUNICIPIO','less_Q1']], on='CD_MUNICIPIO', how='left', suffixes=(False, False))
    
    return data


def generate_levene_statistics(data, field, group):
    # Grouping by city
    city_data_group = data.groupby('CD_MUNICIPIO', as_index = False)
    for city, city_data in city_data_group:
        city_data = generate_percentages_votes(city_data, [field]) 
        print(city_data['id'].unique())
        print(pg.homoscedasticity(city_data, dv=field+'_%', group=group))
        ax = sns.distplot(city_data[field+'_%'])
        plt.show()
    exit()       
    # Associating cities to outliers marks
    #data = data.merge(city_data[['CD_MUNICIPIO','less_Q1']], on='CD_MUNICIPIO', how='left', suffixes=(False, False))
    
    #return #data

def generate_kruskal_wallis_statistics(data,field,alpha=0.05):

    city_data_group = data.groupby('CD_MUNICIPIO', as_index = False)
    same_dist_list = []
    for city, city_data in city_data_group:
        city_data = generate_percentages_votes(city_data, [field]) 
        polling_place_group = city_data.groupby('id')
        polling_place_list = []
        for polling_place, polling_place_data in polling_place_group:
            if(len(polling_place_data[field+'_%'].values) > 1): #Not add polling places with one section
                polling_place_list.append(polling_place_data[field+'_%'].values)

        print(polling_place_list)
        exit()        
        if len(polling_place_list) > 1:    
            args=[pl for pl in polling_place_list]
            H, pval = stats.kruskal(*args)
        else:
            H = -1
            pval = 0
        
        if pval > alpha:
            print(city_data['NM_MUNICIPIO'].values[0]+"- Same distribution")    
            same_dist_list_city = [True] * len(city_data)
        else:
            same_dist_list_city = [False] * len(city_data)
            print(city_data['NM_MUNICIPIO'].values[0]+"- Different distribution")

        same_dist_list = same_dist_list + same_dist_list_city

        

    exit()


    data[field+'_same_dist'] = same_dist_list
    return data      

if __name__ == '__main__':
    #Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    print(project_dir)

    #Set data parammeters
    election_year = '2018'
    office_folder = 'president'
    turn = '2'

    #Set paths
    tse_input_filepath = project_dir + '/data/interim/Brazil/election_data/{}/{}/turn_{}/states/'.format(election_year,office_folder,turn)
    output_filepath = project_dir + '/data/interim/Brazil/election_data/{}/{}/turn_{}/states/'.format(election_year,office_folder,turn)

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    process_data(tse_input_filepath, output_filepath)
