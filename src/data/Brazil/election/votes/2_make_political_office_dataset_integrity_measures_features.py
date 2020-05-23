# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')

from os import listdir
from os.path import isfile, join
from pathlib import Path


from tqdm import tqdm
from scipy import stats

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
    filenames.remove('DF.csv')
    # For each state...
    for filename in tqdm(filenames):
        #logger.info('Processing {} interim data'.format(filename))

        #Loading raw data
        filepath = tse_input_filepath + filename
        data_tse = pd.read_csv(filepath)

        # Generating an id to facilitate grouping
        data_tse['id'] = data_tse['NM_MUNICIPIO'] + data_tse['NR_ZONA'].astype(str)  + data_tse['NR_LOCAL_VOTACAO'].astype(str)

        #Generate capital marks
        data_tse = generate_capitals_mark(data_tse)

        # Generate measure of integrity features
        data_tse = generate_outlier_cities_marks(data_tse)

        # Fill 

        # Save the data as csv
        data_tse.to_csv(output_filepath+filename, index=False)


    logger.info('1.1 - Done!')

def generate_capitals_mark(df_polling_places):

    cities = ['Rio Branco', 'Macapá', 'Manaus', 'Belém', 'Porto Velho','Boa Vista', 
          'Palmas', 'Maceió', 'Salvador', 'Fortaleza', 'São Luís', 'João Pessoa', 
          'Recife', 'Teresina', 'Natal', 'Aracaju', 'Goiânia', 'Cuiabá',
          'Campo Grande', 'Brasília', 'Vitória', 'Belo Horizonte', 'São Paulo', 
          'Rio de Janeiro', 'Curitiba', 'Porto Alegre', 'Florianópolis']

    df_polling_places['capital'] = df_polling_places['NM_MUNICIPIO'].apply(lambda x: True if any(x.lower() == i.lower() for i in cities) else False)
    return df_polling_places



def generate_outlier_cities_marks(data_tse, critical_value=2.575):
    pl_mean_electorate = []
    # Removing capitals
    data_no_capital = data_tse[data_tse['capital'] == False]
    # Grouping by city
    group_data_tse = data_no_capital.groupby('CD_MUNICIPIO')
    for city, city_data in group_data_tse:
        # Grouping by polling place
        polling_places_group = city_data.groupby('id').agg('sum')
        # Calculating mean of electorate per polling place for each city
        pl_mean_electorate = pl_mean_electorate + [(city,city_data['QT_APTOS'].mean())]

    # Generating DataFrame
    pl_mean_electorate_df = pd.DataFrame(pl_mean_electorate, columns=['CD_MUNICIPIO','mean_electorate'])
    # Calculating z-score
    pl_mean_electorate_df['z_score_electorate_no_capital'] = stats.zscore(pl_mean_electorate_df['mean_electorate'])
    # Checking outliers
    pl_mean_electorate_df['negative_outlier'] = pl_mean_electorate_df['z_score_electorate_no_capital'] <= -critical_value
    pl_mean_electorate_df['positive_outlier'] = pl_mean_electorate_df['z_score_electorate_no_capital'] >= +critical_value
    # Associating cities to outliers marks
    data_tse = data_tse.merge(pl_mean_electorate_df, on='CD_MUNICIPIO', how='left')
    return data_tse
    

    


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
