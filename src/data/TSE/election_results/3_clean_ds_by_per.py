# -*- coding: utf-8 -*-
import warnings
import logging
import pandas as pd
from os import mkdir, environ
from dotenv import load_dotenv, find_dotenv

warnings.filterwarnings('ignore')


def clean_data(data_path, output_path, city_lim, l_threshold, precisions, aggr_level, candidates_list, per_dec=5):
    """ Runs scripts to clean interim election data data from (../interim/election_results)
        The results of this script is saved in ../processed/election_results
    """
    logger = logging.getLogger(__name__)
    logger.info('Cleaning data from: \n {}'.format(data_path))
    # Load interim data
    data = pd.read_csv(data_path)
    # Aggregates interim data by aggr_level
    logger.info('Aggregating data by {}'.format(aggr_level))
    output_path, data = aggregate_data(data, aggr_level, candidates_list, output_path)
    # Get the total turnout
    turnout = sum(data['QT_COMPARECIMENTO'])
    # Clean data
    logger.info('Cleaning data')
    if aggr_level != 'City':
        # Input Levenshtein similarity from TSE locations (1 = Highest Value)
        data.loc[data.precision == 'TSE', 'lev_dist'] = 1
        # Get locations with levenshtein similarity greater than a threshold
        data = data[data['lev_dist'] >= l_threshold]
        # Get locations with the precisions specified
        data = data[data['precision'].isin(precisions)]
        # Get locations with the limits specified
        data = data[data['city_limits'].isin(city_lim)]
    # Get the data cleaned turnout
    cl_turnout = sum(data['QT_COMPARECIMENTO'])
    # Calculate the percentage of electorate representation (PER)
    per = calculate_per(turnout, cl_turnout)
    logger.info('Percentage of electorate representation: {}'.format(round(per, per_dec)))
    # Creates folder by percentage of electorate representation (PER)
    folder = 'PER_' + str(round(per, per_dec))
    output_path = create_folder(output_path, folder)
    # Save final dataset
    logger.info('Saving data in:\n{}'.format(output_path + 'data.csv'))
    data.to_csv(output_path + 'data.csv', index=False)
    logger.info('Done!')
    return round(per, per_dec)


def aggregate_data(data, aggr_level, candidates, output_path):
    # Aggregates the data by the aggregation level
    if aggr_level == 'Polling place':
        aggr_data = data.groupby('id_polling_place')
    elif aggr_level == 'Section':
        aggr_data = data.groupby('id_section')
    elif aggr_level == 'Zone':
        aggr_data = data.groupby('id_zone')
    elif aggr_level == 'City':
        aggr_data = data.groupby('id_city')

    # Associates to each column an aggregation function
    aggr_map = {'SG_ UF': 'first',
                'NM_MUNICIPIO': 'first',
                'CD_MUNICIPIO': 'first',
                'COD_LOCALIDADE_IBGE': 'first',
                'NR_ZONA': 'first',
                'NR_LOCAL_VOTACAO': 'first',
                'local_unico': 'first',
                'NR_SECAO': lambda x: x.values.tolist(),
                'rural': 'first',
                'capital': 'first',
                'city_limits': 'first',
                'lev_dist': 'first',
                'QT_APTOS': 'sum',
                'QT_COMPARECIMENTO': 'sum',
                'QT_ABSTENCOES': 'sum',
                'QT_ELEITORES_BIOMETRIA_NH': 'sum',
                'BRANCO': 'sum',
                'NULO': 'sum',
                'lat': 'first',
                'lon': 'first',
                'geometry': 'first',
                'precision': 'first'}

    # Change map function if city, zone or state
    cols = ['NR_LOCAL_VOTACAO', 'local_unico', 'rural', 'capital', 'city_limits', 'lat', 'lon', 'geometry', 'precision']
    if aggr_level == 'City' or aggr_level == 'Zona' or aggr_level == 'State':
        for col in cols:
            aggr_map[col] = lambda x: x.values.tolist()

    # Insert the candidates names and aggregation function
    for c in candidates:
        aggr_map[c] = 'sum'
    # Aggregates the data given the map dictionary
    data = aggr_data.agg(aggr_map)
    # Creates folder with aggregate level name
    folder = 'aggr_by_' + aggr_level.lower().replace(' ', '_')
    output_path = create_folder(output_path, folder)

    return output_path, data


def calculate_per(t, ct):
    return 100 * ct / t


def create_folder(path, folder_name):
    logger = logging.getLogger(__name__)
    path = path + folder_name
    try:
        mkdir(path)
    except FileExistsError:
        logger.info('Folder already exist.')
    return path + '/'


def run(region, year, office_folder, turn, candidates, city_limits, levenshtein_threshold, precision_categories,
        aggregate_level):
    # Find data.env automatically by walking up directories until it's found
    dotenv_path = find_dotenv(filename='data.env')
    # Load up the entries as environment variables
    load_dotenv(dotenv_path)
    # Get data root path
    data_dir = environ.get('ROOT_DATA')
    # Get election results path
    path = data_dir + environ.get('ELECTION_RESULTS')
    # Generate input output paths
    interim_path = path.format(region, year, 'interim')
    processed_path = path.format(region, year, 'processed')
    # Set paths
    input_filepath = interim_path + '/{}/turn_{}/data.csv'.format(office_folder, turn)
    output_filepath = processed_path + '/{}/turn_{}/'.format(office_folder, turn)
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # Print parameters
    print('======Global parameters========')
    print('Election year: {}'.format(year))
    print('Office: {}'.format(office_folder))
    print('Turn: {}'.format(turn))

    # Print parameters
    print('======Filtering parameters========')
    print('City Limits: {}'.format(city_limits))
    print('Levenstein threshold: {}'.format(levenshtein_threshold))
    print('Geocoding precisions: {}'.format(precision_categories))
    print('Aggregate Level: {}'.format(aggregate_level))

    return clean_data(input_filepath,
                      output_filepath,
                      city_limits,
                      levenshtein_threshold,
                      precision_categories,
                      aggregate_level,
                      candidates)
