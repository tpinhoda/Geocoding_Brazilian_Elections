# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')

from os import listdir, mkdir
from os.path import isfile, join
from pathlib import Path
from tqdm import tqdm

import logging

import pandas as pd

def aggregate_data(data, aggregate_level):
    if aggregate_level == 'Polling place':
        aggr_data = data.groupby('id_polling_place')
    elif aggregate_level == 'Section':
        aggr_data = data.groupby('id_section')
    elif aggregate_level == 'Zone':
        aggr_data = data.groupby('id_zone')
    elif aggregate_level == 'City':
        aggr_data = data.groupby('id_city')

    data = aggr_data.agg({'SG_ UF':'first',
                          'NM_MUNICIPIO':'first', 
                          'CD_MUNICIPIO':'first',
                          'COD_LOCALIDADE_IBGE':'first',
                          'NR_ZONA':'first',
                          'NR_LOCAL_VOTACAO':'first',
                          'local_unico':'first',
                          'NR_SECAO':lambda x: [x.values],
                          'rural':'first',
                          'capital':'first',
                          'city_limits':'first',
                          'lev_dist':'first',
                          'QT_APTOS':'sum',
                          'QT_COMPARECIMENTO':'sum',
                          'QT_ABSTENCOES':'sum',
                          'QT_ELEITORES_BIOMETRIA_NH':'sum',
                          'Branco':'sum',
                          'Nulo':'sum',
                          'JAIR BOLSONARO': 'sum',
                          'FERNANDO HADDAD':'sum',
                          'lat':'first',
                          'lon':'first',
                          'geometry':'first',
                          'precision':'first'})

    return(data)    
    
def calculate_integrity(data):

    tse_score = sum(data['precision']  == 'TSE') * 1
    rooftop_score = sum(data['precision'] == 'ROOFTOP') * 0.8
    ri_score = sum(data['precision'] == 'RANGE_INTERPOLATED') * 0.6
    gc_score = sum(data['precision'] == 'GEOMETRIC_CENTER') * 0.4
    approximate_score = sum(data['precision'] == 'APPROXIMATE') * 0.2
    nv_score = sum(data['precision'] == 'NO_VALUE') * 0.0

    precision_score = (tse_score + rooftop_score + ri_score + gc_score + approximate_score + nv_score) / len(data)

    in_score = sum(data['city_limits']  == 'in') * 1

    #Add manually the others
    boundary01_score = sum(data['city_limits']  == 'boundary_0.01') * 0.99
    boundary02_score = sum(data['city_limits']  == 'boundary_0.02') * 0.98
    boundary03_score = sum(data['city_limits']  == 'boundary_0.03') * 0.97
    #

    out_score = sum(data['city_limits']  == 'out') * 0

    city_limits_score = (in_score + out_score + boundary01_score + boundary02_score + boundary03_score) / len(data)

    levenstein_score = sum(data['lev_dist']) / len(data)

    final_score = (precision_score + city_limits_score + levenstein_score) / 3

    return final_score



def filter_data(data, output_filepath, city_limits, levenstein_threshold, precision_categories, aggregate_level):
    """ Runs data processing scripts to generate filtered data from(../prorcessed).
    """
    logger = logging.getLogger(__name__)
    #
    logger.info('Aggregating data by {}'.format(aggregate_level))


    data.loc[data.precision == 'TSE', 'lev_dist'] = 1
    data = data[data['lev_dist'] > levenstein_threshold]
    
    data = data[data['precision'].isin(precision_categories)]
    data = data[data['city_limits'].isin(city_limits)]

    integrity_score = calculate_integrity(data)

    output_filepath = output_filepath+'IS_'+str(round(integrity_score,2))
    try:
        mkdir(output_filepath)
    except:
        print('Folder already exist!')

    data.to_csv(output_filepath+'/data.csv', index=False)

    return integrity_score, data
    logger.info('Done!')


def generate_markdown_report(data, filtered_data, output_filepath, integrity_score, city_limits, levenstein_threshold, precision_categories, aggregate_level):


    parameters_report = { 'Aggregate level': aggregate_level,
                          'City limits:': [city_limits],
                          'Precision categories': [precision_categories],
                          'Levenstein threshold': levenstein_threshold,
    }

    report_df = pd.DataFrame(parameters_report)
    parammeters_markdown = "## Filter Parameters \n" + report_df.to_markdown(showindex=False)


    rows = len(filtered_data)
    rows_perc = 100*rows/len(data)
    rows = str(rows)+' ('+str(round(rows_perc,2))+'%)'

    left_rows = len(data) - len(filtered_data)
    left_rows_perc = 100*left_rows/len(data)
    left_rows = str(left_rows)+' ('+str(round(left_rows_perc,2))+'%)'

    electorate_size = sum(filtered_data['QT_APTOS'])
    electorate_size_perc = 100*electorate_size/sum(data['QT_APTOS'])
    electorate_size = str(electorate_size)+' ('+str(round(electorate_size_perc,2))+'%)'

    left_electorate_size = sum(data['QT_APTOS']) - sum(filtered_data['QT_APTOS'])
    left_electorate_size_perc = 100*left_electorate_size/sum(data['QT_APTOS'])
    left_electorate_size = str(left_electorate_size)+' ('+str(round(left_electorate_size_perc,2))+'%)'

    turnout = sum(filtered_data['QT_COMPARECIMENTO'])
    turnout_perc = 100*turnout/sum(data['QT_COMPARECIMENTO'])
    turnout = str(turnout)+' ('+str(round(turnout_perc,2))+'%)'

    left_turnout = sum(data['QT_COMPARECIMENTO'])-sum(filtered_data['QT_COMPARECIMENTO'])
    left_turnout_perc = 100*left_turnout/sum(data['QT_COMPARECIMENTO'])
    left_turnout = str(left_turnout)+' ('+str(round(left_turnout_perc,2))+'%)'

    nulo = sum(filtered_data['Nulo'])
    nulo_perc = 100*nulo/sum(data['Nulo'])
    nulo = str(nulo)+' ('+str(round(nulo_perc,2))+')'

    branco = sum(filtered_data['Branco'])
    branco_perc = 100*branco/sum(data['Branco'])
    branco = str(branco)+' ('+str(round(branco_perc,2))+'%)'

    bozo = sum(filtered_data['JAIR BOLSONARO'])
    bozo_perc = 100*bozo/sum(data['JAIR BOLSONARO'])
    bozo = str(bozo)+' ('+str(round(bozo_perc,2))+'%)'

    haddad = sum(filtered_data['FERNANDO HADDAD'])
    haddad_perc = 100*haddad/sum(data['FERNANDO HADDAD'])
    haddad = str(haddad)+' ('+str(round(haddad_perc,2))+'%)'


    statistics_report = {aggregate_level: rows,
                         '(Not Included) ' + aggregate_level: left_rows,
                         'Electorate': electorate_size,
                         '(Not Included) Electorate': left_electorate_size,
                         'Turnout': turnout,
                         '(Not Included) Turnout': left_turnout,
    }

    votes_report = {'Null': nulo,
                    'Branco':branco,
                    'Jair Bolsonaro': bozo,
                    'Fernando Haddad': haddad

    }

   # print(statistics_report)
    report_df = pd.DataFrame(statistics_report, index=[0])
    statistics_markdown = "\n ## Summary \n" + report_df.to_markdown(showindex=False)

    report_df = pd.DataFrame(votes_report, index=[0])
    votes_markdown = "\n ## Votes \n" + report_df.to_markdown(showindex=False)

    final_report = '# Dataset: IS_{} \n'.format(round(integrity_score,2)) + parammeters_markdown + '\n' + statistics_markdown + '\n' + votes_markdown

    print(final_report,  file=open(output_filepath+'IS_'+str(round(integrity_score,2)) + '/summary.md' , 'w'))
    

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

    #Set data filters
    city_limits = ['in']
    levenstein_threshold = 0.803
    precision_categories = ['TSE', 'ROOFTOP']
    aggregate_level = 'Polling place'



    #Set paths
    input_filepath = project_dir + '/data/processed/{}/election_data/{}/{}/turn_{}/data.csv'.format(country, election_year, office_folder,turn)
    output_filepath = project_dir + '/data/processed/{}/election_data/{}/{}/turn_{}/filtered/'.format(country, election_year, office_folder,turn)

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    #Print parameters
    print('======Parameters========')
    print('Country: {}'.format(country))
    print('Election year: {}'.format(election_year))
    print('Office: {}'.format(political_office))
    print('Turn: {}'.format(turn))
    
    #Print parameters
    print('======Filtering parameters========')
    print('City Limits: {}'.format(city_limits))
    print('Levenstein threshold: {}'.format(levenstein_threshold))
    print('Geocoding precisions: {}'.format(precision_categories))
    print('Aggregate Level: {}'.format(aggregate_level))
   
    data = pd.read_csv(input_filepath)

    data = aggregate_data(data, aggregate_level)

    integrity_score, data_filtered = filter_data(data, output_filepath, city_limits, levenstein_threshold, precision_categories, aggregate_level)

    generate_markdown_report(data, data_filtered, output_filepath ,integrity_score, city_limits, levenstein_threshold, precision_categories, aggregate_level)