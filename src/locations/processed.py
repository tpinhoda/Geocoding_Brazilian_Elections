import pandas as pd
import geopandas as gpd
import Levenshtein
from os.path import join
from dataclasses import dataclass, field
from pyproj import crs
from shapely.geometry import Point
from src.election import Election
from typing import List
from tqdm import tqdm


CAPITALS = {
            "AC": "RIO BRANCO",
            "AP": "MACAPÁ",
            "AM": "MANAUS",
            "PA": "BELÉM",
            "RO": "PORTO VELHO",
            "RR": "BOA VISTA",
            "TO": "PALMAS",
            "AL": "MACEIÓ",
            "BA": "SALVADOR",
            "CE": "FORTALEZA",
            "MA": "SÃO LUÍS",
            "PB": "JOÃO PESSOA",
            "PE": "RECIFE",
            "PI": "TERESINA",
            "RN": "NATAL",
            "SE": "ARACAJU",
            "GO": "GOIÂNIA",
            "MT": "CUIABÁ",
            "MS": "CAMPO GRANDE",
            "DF": "BRASÍLIA",
            "ES": "VITÓRIA",
            "MG": "BELO HORIZONTE",
            "SP": "SÃO PAULO",
            "RJ": "RIO DE JANEIRO",
            "PR": "CURITIBA",
            "RS": "PORTO ALEGRE",
            "SC": "FLORIANÓPOLIS",
            }

@dataclass
class Processed(Election):
    geocoding_api: str = None
    aggregation_level: str = None
    meshblock_filename: str = None
    meshblock_crs: str = None
    meshblock_col_id: str = None
    city_buffers: List = field(default_factory=list)
    __data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __meshblock: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    
    
    def init_logger_name(self):
        """Initialize the logger name"""
        self.logger_name = "Locations (Processed)"

    def init_state(self):
        """Initialize the  process state name"""
        self.state = "processed"

    def _make_folders(self):
        """Make initial folders"""
        self._make_initial_folders()
        self._mkdir(self.data_name)
        self._mkdir(self.aggregation_level)
    
    def _read_interim_data(self):
        """Read the interim location data file and returns a pandas dataframe."""
        self.logger_info("Reading interim data.")
        filepath = join(
            self._get_process_folder_path(state="interim"),
            self.data_name,
            self.aggregation_level,
            f"locations_{self.geocoding_api}.csv"
        )
      
        self.__data = pd.read_csv(filepath)
    
    def _read_cities_meshblock_data(self):
        """Read the raw cities meshblock data adnd retudns a geopandas dataframe."""
        self.logger_info("Reading cities meshblock data.")
        filename = self.meshblock_filename.split(".")[0]
        filepath = join(self._get_process_folder_path(state='raw'),
                        self.data_name,
                        filename,
                        f"{filename}.shp")
        self.__meshblock = gpd.read_file(filepath)
        self.__meshblock['geometry'] = self.__meshblock['geometry'].to_crs(crs=self.meshblock_crs)
    
    def _save_data(self, filename):
        """save the __data in the iterim folder"""
        self.__data.to_csv(join(self.cur_dir, filename), index=False)
    
    # Calculting precision statistics
    
    def _convert_data_to_geopandas(self):
        geometry = [
            Point((row["[GEO]_LONGITUDE"], row[["[GEO]_LATITUDE"]])) for _, row in self.__data.iterrows()
        ]
        self.__data = gpd.GeoDataFrame(self.__data, geometry=geometry, crs=self.meshblock_crs)
        
    
    def _generate_city_limits_measure(self):
        self.logger_info("Generating city limits measure")
        # Convert df_polling places to a geopandas dataframe
        self._convert_data_to_geopandas()
        # Checking if coordinates are inside city boundaries
        in_out_list = []
        for _, location in tqdm(self.__data.iterrows(), total=len(self.__data), leave=False):
            # Get the city map 
            city = self.__meshblock[self.__meshblock[self.meshblock_col_id] == str(location['[GEO]_ID_IBGE_CITY'])]
            # Check if point is inside polygon
            if city['geometry'].contains(location['geometry']).bool():
                in_out_list += ["in"]
                inside = True
            else:
                inside = False
                b = 0
                while not inside and b < len(self.city_buffers):
                    buffered_city = city.to_crs('+proj=cea').buffer(self.city_buffers[b]).to_crs(crs=self.meshblock_crs)
                    if buffered_city.contains(location['geometry']).bool():
                        inside = True
                        in_out_list += [f"boundary_{self.city_buffers[b]}"]
                    else:
                        b += 1
            if not inside:
                in_out_list += ["out"]            

        self.__data["[GEO]_CITY_LIMITS"] = in_out_list
    
    def _generate_levenshtein_measure(self):
        """Generate levenshtein measure."""
        self.logger_info("Generating Levenshtein similarity measure.")
        self.__data["[GEO]_LEVENSHTEIN_SIMILARITY"] = self.__data.apply(
            lambda x: Levenshtein.ratio(
                x["[GEO]_QUERY_ADDRESS"].lower(), x["[GEO]_FETCHED_ADDRESS"].lower()
            ),
            axis=1,
        )
    
    def _generate_rural_areas_mark(self):

        self.logger_info("Generating rural areas marks.")
        searchfor = ["rural", "povoado", "pov.", "comunidade", "localidade", "km", "sitio"]
        self.__data["[GEO]_RURAL_MARKS"] = self.__data[
            "[GEO]_QUERY_ADDRESS"
        ].apply(lambda x: any(i in x.lower() for i in searchfor))
    
    def generate_capitals_mark(self):
        self.logger_info("Generating capital cities marks.")
        capitals_l = []
        for _, row in tqdm(self.__data.iterrows(), leave=False):
            if row["[GEO]_CITY"] == CAPITALS[row["[GEO]_UF"]]:
                capitals_l.append(True)
            else:
                capitals_l.append(False)
        self.__data["[GEO]_CAPITAL_MARKS"] = capitals_l

    def run(self):
        self.init_logger_name()
        self.init_state()
        self.logger_info("Generating processed data.")
        self._make_folders()
        self._read_interim_data()
        self._read_cities_meshblock_data()
        self._generate_city_limits_measure()
        self._generate_levenshtein_measure()
        self._generate_rural_areas_mark()
        self.generate_capitals_mark()
        self._save_data(f"locations_{self.geocoding_api}.csv")