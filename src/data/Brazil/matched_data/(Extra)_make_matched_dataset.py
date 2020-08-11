# -*- coding: utf-8 -*-
# To run this script the 1_make_political_office_dataset.py should be already executed
import logging
import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point


def aggregate_data(data, aggregate_level):
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
                'Branco': 'sum',
                'Nulo': 'sum',
                'JAIR BOLSONARO': 'sum',
                'FERNANDO HADDAD': 'sum',
                'lat': 'first',
                'lon': 'first',
                'geometry': 'first',
                'precision': 'first'}

    if aggregate_level == 'Polling place':
        aggr_data = data.groupby('id_polling_place')
    elif aggregate_level == 'Section':
        aggr_data = data.groupby('id_section')
    elif aggregate_level == 'Zone':
        aggr_data = data.groupby('id_zone')
    elif aggregate_level == 'City':
        aggr_data = data.groupby('id_city')
    elif aggregate_level == 'Cod_ap':
        aggr_data = data.groupby('Cod_ap')
        del aggr_map['lat']
        del aggr_map['lon']
        del aggr_map['geometry']
        aggr_map['precision'] = lambda x: x.values.tolist()
        aggr_map['NR_LOCAL_VOTACAO'] = lambda x: x.values.tolist()
        aggr_map['NR_ZONA'] = lambda x: x.values.tolist()

    data = aggr_data.agg(aggr_map)

    return(data)


def filter_data(input_filepath, digital_mesh_filepath, output_filepath, state):
    """ Runs data processing scripts to turn raw election data data from (../raw) into
        structured data filtered by political office (saved in ../interim).
    """
    logger = logging.getLogger(__name__)
    logger.info('Structuring data...')

    digital_mesh = gpd.read_file(digital_mesh_filepath, driver='GeoJSON')
    digital_mesh.dropna(inplace=True)
    data = pd.read_csv(input_filepath)
    data = data[data['SG_ UF'] == state]
    geometry = [Point((row.lon, row.lat)) for index, row in data.iterrows()]
    data = gpd.GeoDataFrame(data, geometry=geometry)
    data.crs = {'init': 'epsg:4674'}
    data_joined = gpd.sjoin(left_df=data, right_df=digital_mesh, how='left', op='within')

#    cod_ap = []
#    for index, polling_place in tqdm(data.iterrows(), total=data.shape[0]):
#        pl_coordinates = polling_place.geometry
#        ap = []
#        for index, weightening_area in digital_mesh.iterrows():
#            wa_coordinates = weightening_area.geometry
#            wa_cod = weightening_area.Cod_ap
#            try:
#                if wa_coordinates.contains(pl_coordinates):
#                    ap.append(wa_cod)
#            except:
#                print('Error in: {}'.format(weightening_area['Cod_ap']))
#        if len(ap) == 1:
#            cod_ap = cod_ap + ap
#        elif len(ap) > 1:
#            print("Deu Ruim")
#            exit()
#        else:
#            print(polling_place['geometry'])
#            cod_ap.append(0)
    # Listing raw data
    print('Data {} - Joined {}'.format(len(data), len(data_joined)))
#    data['Cod_ap'] = cod_ap

    data_joined.to_csv(output_filepath + 'polling_place_aggr.csv', index=False)
    data_joined = aggregate_data(data_joined, 'Cod_ap')
    data_joined.to_csv(output_filepath + 'weighting_area_aggr.csv')
    logger.info('Done!')


if __name__ == '__main__':
    # Project path
    project_dir = str(Path(__file__).resolve().parents[5])
    print(project_dir)

    # Set data parammeters
    country = 'Brazil'
    election_year = '2018'
    political_office = 'president'
    turn = '2'
    state = 'RS'
    census_year = '2010'
    IS = '0.99946'
    # Set paths
    input_filepath = project_dir + '/data/processed/{}/election_data/{}/{}/turn_{}/filtered/by_state/{}/IS_{}/data.csv'.format(country, election_year,political_office,turn,state,IS)
    digital_mesh_filepath = project_dir + '/data/interim/{}/census_data/{}/weightening_area/digital_mesh/{}.json'.format(country, census_year, state)
    output_filepath = project_dir + '/data/processed/{}/election_data/{}/{}/turn_{}/matched_census/by_state/{}/IS_{}/data/'.format(country, election_year,political_office,turn,state,IS)

    # Print parameters
    print('======Parameters========')
    print('Country: {}'.format(country))
    print('Election year: {}'.format(election_year))
    print('Political office: {}'.format(political_office))
    print('Turn: {}'.format(turn))
    print('State: {}'.format(state))

    # Log text to show on screen
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    filter_data(input_filepath, digital_mesh_filepath, output_filepath, state)
