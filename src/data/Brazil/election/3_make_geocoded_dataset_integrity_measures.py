# -*- coding: utf-8 -*-

# To run this script the 2_make_geocoded_dataset.py should be already executed

from os import listdir
from os.path import isfile, join
from pathlib import Path
from shapely.geometry import Point
from tqdm import tqdm

import logging
import pandas as pd
import geopandas as gpd
import Levenshtein

def generate_city_limits_measure(df_polling_places, df_cities_maps, buffer_values):
	logger = logging.getLogger(__name__)
	logger.info('1 - Generating city limits measure')

	#Convert df_polling places to a geopandas dataframe
	logger.info('1.1 - Converting dataframe to geopandas dataframe')
	geometry = [ Point( ( row.lon, row.lat ) ) for index, row in df_polling_places.iterrows() ]
	df_polling_places = gpd.GeoDataFrame(df_polling_places, geometry = geometry)
	df_polling_places.crs = {'init': 'epsg:4674'}

	#Checking if coordinates are inside city boundaries
	logger.info('1.2 - Checking if coordinates are inside cities')	
	to_concat = []
	for index, location in tqdm(df_polling_places.iterrows(), total=df_polling_places.shape[0], leave=False):
    	#Get the city map 
	    city = df_cities_maps[df_cities_maps.CD_GEOCMU == str(location['COD_LOCALIDADE_IBGE'])].reset_index(drop=True)
	    
	    #Get the city geometry
	    city_polygon = city['geometry'].values[0]
	    
	    #Get the coordinate to check
	    point = location['geometry']

	    #Check if point is inside polygon
	    if point.within(city_polygon):
	        to_concat = to_concat + ['in']
	    else:
	        to_concat = to_concat + ['out']
	
	#Saving results in the attribute city_limits
	df_polling_places['city_limits'] = to_concat

	#Sort the buffer values in descending order
	buffer_values.sort(reverse=True)

	#Checking if coordinates are inside city frontiers for each buffer value
	for b in buffer_values:
	
		logger.info('1.3 - Buffering cities maps')
		
		#Create a copy of the city maps to apply the buffer
		df_buffered_cities_maps = df_cities_maps.copy()

		#Applying buffer to the maps
		df_buffered_cities_maps['geometry'] = df_buffered_cities_maps['geometry'].buffer(b)

		#Checking if coordinates are inside city frontiers of size equal to buffer value	
		logger.info('1.4 - Checking if coordinates are in cities fronties of {}'.format(b))		
		for index, location in tqdm(df_polling_places.iterrows(), total=df_polling_places.shape[0], leave=False):
	    	
	    	#Get city map
		    city = df_buffered_cities_maps[df_buffered_cities_maps.CD_GEOCMU == str(location['COD_LOCALIDADE_IBGE'])].reset_index(drop=True)
		    
		    #Get city geometry
		    city_polygon = city['geometry'].values[0]
		    
		    #Get point to check
		    point = location['geometry']

		    #Get if in the first check the point was inside or out the city limits
		    city_limits = location['city_limits']

		    #Check if the point is in the frontier
		    if point.within(city_polygon) and city_limits != 'in':
		    	df_polling_places.at[index,'city_limits'] = 'boundary_' + str(b)

	logger.info('1.5 - Done!')	    	
	return df_polling_places


def generate_levenshtein_measure(df_polling_places):

	logger = logging.getLogger(__name__)
	logger.info('2 - Generating Levenshtein similarity measure')

	#Create query string
	df_polling_places['query_address'] = df_polling_places['ENDERECO'] +', '+ \
										 df_polling_places['BAIRRO_LOCAL_VOT'] + ' - ' + \
										 df_polling_places['LOCALIDADE_LOCAL_VOTACAO'] + ' - ' + \
										 df_polling_places['SGL_UF'] + ', ' + \
										 df_polling_places['CEP'].astype('str') + ', Brasil'

	#Generate levenshtein measure
	logger.info('2.1 - Calculating Levenshtein similarity measure')
	tqdm.pandas(leave=False)								 
	df_polling_places['lev_dist'] = df_polling_places.progress_apply(lambda x: Levenshtein.ratio(x['query_address'].lower(), x['fetched_address'].lower()), axis=1)
	
	logger.info('2.2 - Done!')
	return df_polling_places

def generate_rural_areas_mark(df_polling_places):
	logger = logging.getLogger(__name__)
	logger.info('3 - Generating rural areas marks')

	#Create query string
	df_polling_places['query_address'] = df_polling_places['ENDERECO'] +', '+ \
										 df_polling_places['BAIRRO_LOCAL_VOT'] + ' - ' + \
										 df_polling_places['LOCALIDADE_LOCAL_VOTACAO'] + ' - ' + \
										 df_polling_places['SGL_UF'] + ', ' + \
										 df_polling_places['CEP'].astype('str') + ', Brasil'

	#Words associated to rural areas
	searchfor = ['rural', 'povoado', 'pov.'  'comunidade', 'localidade', 'km', 'sitio']

	logger.info('3.1 - Checking rural areas')
	tqdm.pandas(leave=False)
	df_polling_places['rural'] = df_polling_places['query_address'].progress_apply(lambda x: True if any(i in x.lower() for i in searchfor) else False)

	logger.info('3.2 - Done!')
	return df_polling_places


def generate_capitals_mark(df_polling_places):
	logger = logging.getLogger(__name__)
	logger.info('5 - Generating capital cities marks')

	cities = ['Rio Branco', 'Macapá', 'Manaus', 'Belém', 'Porto Velho','Boa Vista', 
          'Palmas', 'Maceió', 'Salvador', 'Fortaleza', 'São Luís', 'João Pessoa', 
          'Recife', 'Teresina', 'Natal', 'Aracaju', 'Goiânia', 'Cuiabá',
          'Campo Grande', 'Brasília', 'Vitória', 'Belo Horizonte', 'São Paulo', 
          'Rio de Janeiro', 'Curitiba', 'Porto Alegre', 'Florianópolis']

	logger.info('5.1 - Checking polling places in capitals')
	tqdm.pandas(leave=False)
	df_polling_places['capital'] = df_polling_places['LOCALIDADE_LOCAL_VOTACAO'].progress_apply(lambda x: True if any(x.lower() == i.lower() for i in cities) else False)
	
	logger.info('5.2 - Done!')
	return df_polling_places

if __name__ == '__main__':
    #Project path
    project_dir = str(Path(__file__).resolve().parents[4])
    print(project_dir)

    #Set data parammeters
    election_year = '2018'
 
    #Set paths
    data_filepath = project_dir + '/data/interim/Brazil/election_data/{}/polling_places/polling_places.csv'.format(election_year)
    cities_maps_filepath = project_dir + '/data/raw/Brazil/census_2010/digital_mesh/municipios/BRMUE250GC_SIR.shp'
    output_filepath = project_dir + '/data/processed/Brazil/election_data/{}/polling_places/polling_places.csv'.format(election_year)

    #Read data
    df_polling_places = pd.read_csv(data_filepath)
    df_cities_maps =  gpd.read_file(cities_maps_filepath,  encoding = 'utf-8')

    #Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    #Check if the coordinates are inside the city limits
    df_polling_places = generate_city_limits_measure(df_polling_places, df_cities_maps, buffer_values=[.03, .02, .01])

    #Check similarity between fetched and queried addresses
    df_polling_places = generate_levenshtein_measure(df_polling_places)

    #Check for rural areas
    df_polling_places = generate_rural_areas_mark(df_polling_places)

    #Check for capitals
    df_polling_places = generate_capitals_mark(df_polling_places)


    

    logger = logging.getLogger(__name__)
    logger.info('6 - Writting csv')
    df_polling_places.to_csv(output_filepath, index=False)
    logger.info('6.1 - Done!')

