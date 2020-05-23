# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')

from os import listdir
from os.path import isfile, join
from pathlib import Path

import logging
from tqdm import tqdm

import pandas as pd


def process_data(tse_input_filepath, geo_input_filepath, output_filepath):

	""" Runs data processing scripts to turn interim election data data from (../interim) into
		processed data with geocoding by polling place (saved in ../processed).
	"""
	logger = logging.getLogger(__name__)
	logger.info('1 - Processing data')

	# Loading data with geocode information from polling places
	data_geo = pd.read_csv(geo_input_filepath)

	# Generating an id to facilitate merging
	data_geo['id'] = data_geo['LOCALIDADE_LOCAL_VOTACAO'] + data_geo['ZONA'].astype(str)  + data_geo['NUM_LOCAL'].astype(str)

	# Listing raw data
	filenames = [filename for filename in listdir(tse_input_filepath) if isfile(join(tse_input_filepath, filename))]

	# For each state...
	for filename in tqdm(filenames):
		#logger.info('Processing {} interim data'.format(filename))

		#Loading raw data
		filepath = tse_input_filepath + filename
		data_tse = pd.read_csv(filepath)

		# Generating an id to facilitate merging
		data_tse['id'] = data_tse['NM_MUNICIPIO'] + data_tse['NR_ZONA'].astype(str)  + data_tse['NR_LOCAL_VOTACAO'].astype(str)

		# Merge datasets
		merged = data_tse.merge(data_geo[['id','COD_LOCALIDADE_IBGE','local_unico','lat','lon','geometry','rural','capital','precision', 'lev_dist', 'city_limits']], on='id', how='left')

		# Save the data as csv
		merged.to_csv(output_filepath+filename, index=False)

	logger.info('1.1 - Done!')



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
	geo_input_filepath = project_dir + '/data/processed/Brazil/election_data/{}/polling_places/polling_places.csv'.format(election_year)
	output_filepath = project_dir + '/data/processed/Brazil/election_data/{}/{}/turn_{}/states/'.format(election_year,office_folder,turn)

	#Log text to show on screen
	log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
	logging.basicConfig(level=logging.INFO, format=log_fmt)

	process_data(tse_input_filepath, geo_input_filepath, output_filepath)
