import coloredlogs, logging
coloredlogs.install()
log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_fmt)

import pandas as pd
import numpy as np
import googlemaps
from os.path import join
from dataclasses import dataclass, field
from src.election import Election
from typing import Dict
from tqdm  import tqdm

@dataclass
class Interim(Election):
    
    gmaps_key: str
    save_at: int = 10
    data: pd.DataFrame = field(default_factory=pd.DataFrame)
    precision_order: Dict[str, int] = field(default_factory=dict)
    
    def _read_csv(self):
        """Read the polling places.csv file and returns a pandas dataframe"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Reading data.")
        dtype = {'SGL_UF':'str', 'COD_LOCALIDADE_IBGE':'str', 'LOCALIDADE_LOCAL_VOTACAO':'str', 'ZONA':'str',
        'BAIRRO_ZONA_SEDE':'str', 'LATITUDE_ZONA':'float', 'LONGITUDE_ZONA':'float',
        'NUM_LOCAL':'str', 'SITUACAO_LOCAL':'str', 'TIPO_LOCAL':'str', 'LOCAL_VOTACAO':'str', 
        'ENDERECO':'str', 'BAIRRO_LOCAL_VOT':'str', 'CEP':'str', 'LATITUDE_LOCAL':'float', 
        'LONGITUDE_LOCAL':'float', 'NUM_SECAO':'str', 'SECAO_AGREGADORA':'str', 'SECAO_AGREGADA':'str'
        }
        filepath = (join(self._get_state_folders_path(state = 'raw'), self.data_type, self.data_type + '.csv' ))
        self.data =  pd.read_csv(filepath, encoding='Latin5', sep=';',decimal=',', dtype=dtype)
    
    def _clean_addresses(self):
        """Remove unecessary information from addrresses"""
        self.data['CLEAN_ADDRESS'] =  self.data['ENDERECO'].str.replace(" - ZONA URBANA","")
        self.data['CLEAN_ADDRESS'] =  self.data['ENDERECO'].str.replace(" - ZONA RURAL","")

    def _filter_by_unique_id(self):
        """Generate a unique id for each polling place"""
        self.data['ID'] = self.data['LOCAL_VOTACAO'] + self.data['ENDERECO'] + self.data['LOCALIDADE_LOCAL_VOTACAO'] + self.data['SGL_UF']
        self.data = self.data.drop_duplicates(subset='ID').reset_index()
    
    def _remove_foreign_places(self):
        """Remove places that are out of the region"""
        self.data = self.data[self.data['SGL_UF'] != 'ZZ']
    
    def _create_precision_attribute(self):
        """Create the precision attribute regarding the latitude and longitude"""
        self.data['PRECISION'] = np.where(np.isnan(self.data['LATITUDE_LOCAL']), 'NO_VALUE', 'TSE')

    def _create_fetched_attribute(self):
        """Create the fetched address regarding the latitude and longitude"""
        self.data['FETCHED_ADDRESS'] = np.where(self.data['PRECISION'] != 'TSE', 'NO_VALUE', self.data['CLEAN_ADDRESS'])
    
    def _save_data(self):
        """save the data in the iterim folder"""
        self.data.to_csv(join(self.cur_dir, 'polling_places.csv'), index=False)
    
    def _preprocessing_data(self):
        """Pre-processing of the polling places """
        logger = logging.getLogger(self.logger_name)
        logger.info("Pre-Processing data.")
        self._read_csv()
        self._clean_addresses()
        self._filter_by_unique_id()
        self._remove_foreign_places()
        self._create_precision_attribute()
        self._create_fetched_attribute()
        
    def _geocoding_data(self):
        """Get coordinates for each row in data"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Geocoding data.")
        gmaps = googlemaps.Client(key=self.gmaps_key, queries_per_second=40)
        for count_rows, (index, row) in tqdm(enumerate(self.data.iterrows()),  total=len(self.data), desc='Geocoding'):
            if row['PRECISION'] == 'NO_VALUE':
                try:
                    components = {'country': self.region, 'administrative_area': row['LOCALIDADE_LOCAL_VOTACAO']}
                    address = row['LOCAL_VOTACAO'] + ', ' + row['CLEAN_ADDRESS'] + ', ' + row['LOCALIDADE_LOCAL_VOTACAO'] +', '+row['SGL_UF'] + self.region
                    result = gmaps.geocode(language='pt-BR', address= address, components=components)
                    if not result:
                        continue
                    self.data.loc[index, 'LATITUDE_LOCAL'] = result[0]['geometry']['location']['lat']
                    self.data.loc[index, 'LONGITUDE_LOCAL'] = result[0]['geometry']['location']['lng']
                    self.data.loc[index, 'PRECISION'] = result[0]['geometry']['location_type']
                    self.data.loc[index, 'FETCHED_ADDRESS'] =  result[0]['formatted_address']
                except Exception as e:
                    print(e)
            if not (count_rows + 1) % self.save_at:
                self._save_data()
                break
        
    def generate_interim_data(self):
        """Generates interim data regarding the polling places"""
        self._initialize_folders()
        self._preprocessing_data()
        self._geocoding_data()
    
            
root = "H:\Google Drive\Doutorado\EFD\data"
gmaps= "AIzaSyD-M4qwD5vb3nusP9cBgnrm4Jq5A8DjEHA"
election_results = Interim("Brazil", "TSE", "2018", "2", "polling_places", "Interim", root, root, "interim", gmaps)
election_results.generate_interim_data()
print(election_results)


#election_raw = election_results.Raw(url_data)
#election_raw.generate_raw_data()
