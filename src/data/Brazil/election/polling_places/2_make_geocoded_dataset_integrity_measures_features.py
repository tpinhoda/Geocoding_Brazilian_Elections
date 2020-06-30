# -*- coding: utf-8 -*-
# To run this script the 2_make_geocoded_dataset.py should be already executed
import warnings
import logging
import pandas as pd
import geopandas as gpd
import Levenshtein
from pathlib import Path
from shapely.geometry import Point
from tqdm import tqdm

warnings.filterwarnings('ignore')


def generate_city_limits_measure(df_polling_places,
                                 df_cities_maps,
                                 buffer_values):

    logger = logging.getLogger(__name__)
    logger.info('1 - Generating city limits measure')
    # Convert df_polling places to a geopandas dataframe
    logger.info('1.1 - Converting dataframe to geopandas dataframe')

    geometry = [Point((row.lon, row.lat))
                for index, row in df_polling_places.iterrows()]

    df_polling_places = gpd.GeoDataFrame(df_polling_places,
                                         geometry=geometry)
    df_polling_places.crs = {'init': 'epsg:4674'}
    # Checking if coordinates are inside city boundaries
    logger.info('1.2 - Checking if coordinates are inside cities')
    to_concat = []
    for index, location in tqdm(df_polling_places.iterrows(),
                                total=df_polling_places.shape[0],
                                leave=False):
        # Get the city map
        city = df_cities_maps.CD_GEOCMU == str(location['COD_LOCALIDADE_IBGE'])
        city = df_cities_maps[city].reset_index(drop=True)
        # Get the city geometry
        city_polygon = city['geometry'].values[0]
        # Get the coordinate to check
        point = location['geometry']
        # Check if point is inside polygon
        if point.within(city_polygon):
            to_concat = to_concat + ['in']
        else:
            to_concat = to_concat + ['out']
    # Saving results in the attribute city_limits
    df_polling_places['city_limits'] = to_concat

    # Sort the buffer values in descending order
    buffer_values.sort(reverse=True)

    # Checking if coordinates are inside city frontiers for each buffer value
    info = '1.3 - Checking if coordinates are in cities with buffered frontier'
    logger.info(info)
    step = 0
    for b in buffer_values:
        step += 1
        # Create a copy of the city maps to apply the buffer
        df_buffered_cities = df_cities_maps.copy()

        # Applying buffer to the maps
        buffered_cities = df_buffered_cities['geometry'].buffer(b)
        df_buffered_cities['geometry'] = buffered_cities

        # Checking if coordinates are inside city buffered frontiers
        logger.info('1.3.{} Checking for buffer size {}'.format(step, b))
        for index, location in tqdm(df_polling_places.iterrows(),
                                    total=df_polling_places.shape[0],
                                    leave=False):
            # Get city map
            locations = str(location['COD_LOCALIDADE_IBGE'])
            city = df_buffered_cities.CD_GEOCMU == locations
            city = df_buffered_cities[city].reset_index(drop=True)
            # Get city geometry
            city_polygon = city['geometry'].values[0]
            # Get point to check
            point = location['geometry']
            # Check if in the first check the point was in or out the city
            city_limits = location['city_limits']

            # Check if the point is in the frontier
            if point.within(city_polygon) and city_limits != 'in':
                label = 'boundary_' + str(b)
                df_polling_places.at[index, 'city_limits'] = label

    logger.info('Done!')
    return df_polling_places


def generate_levenshtein_measure(df_polling_places):
    logger = logging.getLogger(__name__)
    logger.info('2 - Generating Levenshtein similarity measure')

    # Create query string
    address = df_polling_places['ENDERECO']
    neighb = df_polling_places['BAIRRO_LOCAL_VOT']
    city = df_polling_places['LOCALIDADE_LOCAL_VOTACAO']
    state = df_polling_places['SGL_UF']
    zip_code = df_polling_places['CEP'].astype('str')
    country = 'Brasil'
    query = address + ',' + neighb + ',' + city + ',' + state + ',' + zip_code + ',' + country
    df_polling_places['query_address'] = query
    # Generate levenshtein measure
    logger.info('2.1 - Calculating Levenshtein similarity measure')
    tqdm.pandas(leave=False)
    df_polling_places['lev_dist'] = df_polling_places.progress_apply(lambda x: Levenshtein.ratio(x['query_address'].lower(),
                                                                                                 x['fetched_address'].lower()),
                                                                     axis=1)

    logger.info('Done!')
    return df_polling_places


def generate_rural_areas_mark(df_polling_places):
    logger = logging.getLogger(__name__)
    logger.info('3 - Generating rural areas marks')

    # Create query string
    address = df_polling_places['ENDERECO']
    neighb = df_polling_places['BAIRRO_LOCAL_VOT']
    city = df_polling_places['LOCALIDADE_LOCAL_VOTACAO']
    state = df_polling_places['SGL_UF']
    zip_code = df_polling_places['CEP'].astype('str')
    country = 'Brasil'
    query = address + ',' + neighb + ',' + city + ',' + state + ',' + zip_code + ',' + country
    df_polling_places['query_address'] = query
    # Words associated to rural areas
    searchfor = ['rural',
                 'povoado',
                 'pov.',
                 'comunidade',
                 'localidade',
                 'km',
                 'sitio']

    logger.info('3.1 - Checking rural areas')
    tqdm.pandas(leave=False)
    df_polling_places['rural'] = df_polling_places['query_address'].progress_apply(lambda x: True
                                                                                   if any(i in x.lower() for i in searchfor)
                                                                                   else False)

    logger.info('Done!')
    return df_polling_places


def generate_capitals_mark(df_polling_places):
    logger = logging.getLogger(__name__)
    logger.info('5 - Generating capital cities marks')

    capitals = {'AC': 'RIO BRANCO',
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
                'SC': 'FLORINÓPOLIS'}
    capitals_l = []
    logger.info('5.1 - Checking polling places in capitals')
    for index, row in tqdm(df_polling_places.iterrows(), leave=False):
        if row['LOCALIDADE_LOCAL_VOTACAO'] == capitals[row['SGL_UF']]:
            capitals_l.append(True)
        else:
            capitals_l.append(False)

    df_polling_places['capital'] = capitals_l
    logger.info('Done!')
    return df_polling_places


if __name__ == '__main__':
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    print(project_dir)
    # Set data parammeters
    election_year = '2018'
    buffers = [.03, .02, .01]
    # Set paths
    data_filepath = project_dir + '/data/interim/Brazil/TSE/election_data/{}/polling_places/polling_places.csv'.format(election_year)
    cities_maps_filepath = project_dir + '/data/raw/Brazil/IBGE/digital_mesh/city/BRMUE250GC_SIR.shp'
    output_filepath = project_dir + '/data/processed/Brazil/TSE/election_data/{}/polling_places/polling_places.csv'.format(election_year)
    # Read data
    df_polling_places = pd.read_csv(data_filepath)
    df_cities_maps = gpd.read_file(cities_maps_filepath, encoding='utf-8')
    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)
    # Check if the coordinates are inside the city limits
    df_polling_places = generate_city_limits_measure(df_polling_places,
                                                     df_cities_maps,
                                                     buffer_values=buffers)
    # Check similarity between fetched and queried addresses
    df_polling_places = generate_levenshtein_measure(df_polling_places)
    # Check for rural areas
    df_polling_places = generate_rural_areas_mark(df_polling_places)
    # Check for capitals
    df_polling_places = generate_capitals_mark(df_polling_places)
    logger = logging.getLogger(__name__)
    logger.info('6 - Writting csv')
    df_polling_places.to_csv(output_filepath, index=False)
    logger.info('6.1 - Done!')
