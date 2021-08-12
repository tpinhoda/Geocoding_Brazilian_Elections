"""Generates processed data for locations."""
from os.path import join
from dataclasses import dataclass, field
from typing import List
import pandas as pd
import geopandas as gpd
import Levenshtein
from tqdm import tqdm
from shapely.geometry import Point
from src.election import Election

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
    """Represents the Brazilian polling places in processed state of processing.

    This object pre-processes and geocodes the Brazilian polling places.

    Attributes
    ----------
        aggregation_level: str
            The level of aggregation [polling_places:, neighborhood, city]
        geocoding_api: str
            The geocoding api to be used (Google Maps: GMAPS, OpenStreep Map: OSM)
        meshblock_filename: str
            Meshblock filename
        meshblock_crs: str
            Meshblock coordinate system
        meshblock_col_id: str
            Meshblock id column
        city_buffers: List
            List of city buffers
    """

    geocoding_api: str = None
    aggregation_level: str = None
    meshblock_filename: str = None
    meshblock_crs: str = None
    meshblock_col_id: str = None
    city_buffers: List = field(default_factory=list)
    __data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __meshblock: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    def _read_interim_data(self):
        """Read the interim location data file and returns a pandas dataframe."""
        self.logger_info("Reading interim data.")
        filepath = join(
            self._get_process_folder_path(state="interim"),
            self.data_name,
            self.aggregation_level,
            f"locations_{self.geocoding_api}.csv",
        )

        self.__data = pd.read_csv(filepath, low_memory=False)

    def _read_cities_meshblock_data(self):
        """Read the raw cities meshblock data adnd retudns a geopandas dataframe."""
        self.logger_info("Reading cities meshblock data.")
        filename = self.meshblock_filename.split(".")[0]
        filepath = join(
            self._get_process_folder_path(state="raw"),
            self.data_name,
            filename,
            f"{filename}.shp",
        )
        self.__meshblock = gpd.read_file(filepath)
        self.__meshblock["geometry"] = self.__meshblock["geometry"].to_crs(
            crs=self.meshblock_crs
        )

    def _save_data(self, filename):
        """save the __data in the iterim folder"""
        self.logger_info("Saving file.")
        self.__data.to_csv(join(self.cur_dir, filename), index=False)

    # Calculating precision statistics
    def _convert_data_to_geopandas(self):
        """Convert pandas to geopandas dataframe."""
        self.logger_info("Converting data to geodataframe.")
        geometry = [
            Point((row["[GEO]_LONGITUDE"], row[["[GEO]_LATITUDE"]]))
            for _, row in self.__data.iterrows()
        ]
        self.__data = gpd.GeoDataFrame(
            self.__data, geometry=geometry, crs=self.meshblock_crs
        )

    def _generate_city_limits_measure(self):
        """Generate city limit measure."""
        self.logger_info("Generating city limits measure")
        # Convert df_polling places to a geopandas dataframe
        self._convert_data_to_geopandas()
        # Checking if coordinates are inside city boundaries
        in_out_list = []
        for _, location in tqdm(
            self.__data.iterrows(), total=len(self.__data), leave=True
        ):
            city = self.__meshblock[
                self.__meshblock[self.meshblock_col_id]
                == str(location["[GEO]_ID_IBGE_CITY"])
            ]
            # Check if point is inside psolygon
            if city["geometry"].contains(location["geometry"]).bool():
                in_out_list += ["in"]
                inside = True
            else:
                inside = False
                buffer_id = 0
                while not inside and buffer_id < len(self.city_buffers):
                    buffered_city = (
                        city.to_crs("+proj=cea")
                        .buffer(self.city_buffers[buffer_id])
                        .to_crs(crs=self.meshblock_crs)
                    )
                    if buffered_city.contains(location["geometry"]).bool():
                        inside = True
                        in_out_list += [f"boundary_{self.city_buffers[buffer_id]}"]
                    else:
                        buffer_id += 1
            if not inside:
                in_out_list += ["out"]

        self.__data["[GEO]_CITY_LIMITS"] = in_out_list

    def _generate_levenshtein_measure(self):
        """Generate levenshtein measure."""
        self.logger_info("Generating levenshtein similarity measure.")
        self.__data["[GEO]_LEVENSHTEIN_SIMILARITY"] = self.__data.apply(
            lambda x: Levenshtein.ratio(
                x["[GEO]_QUERY_ADDRESS"].lower(), x["[GEO]_FETCHED_ADDRESS"].lower()
            ),
            axis=1,
        )

    def _generate_rural_areas_mark(self):
        """Generates rural areas marks"""
        self.logger_info("Generating rural areas marks.")
        searchfor = [
            "rural",
            "povoado",
            "pov.",
            "comunidade",
            "localidade",
            "km",
            "sitio",
        ]
        self.__data["[GEO]_RURAL_MARKS"] = self.__data["[GEO]_QUERY_ADDRESS"].apply(
            lambda x: any(i in x.lower() for i in searchfor)
        )

    def _generate_capitals_mark(self):
        """Generates capital marks"""
        self.logger_info("Generating capital cities marks.")
        capitals_l = []
        for _, row in tqdm(self.__data.iterrows(), leave=False):
            if row["[GEO]_CITY"] == CAPITALS[row["[GEO]_UF"]]:
                capitals_l.append(True)
            else:
                capitals_l.append(False)
        self.__data["[GEO]_CAPITAL_MARKS"] = capitals_l

    def run(self):
        """Run state process"""
        self.init_logger_name(msg="Locations (Processed)")
        self.init_state(state="processed")
        self.logger_info("Generating processed data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        self._read_interim_data()
        self._read_cities_meshblock_data()
        self._generate_city_limits_measure()
        self._generate_levenshtein_measure()
        self._generate_rural_areas_mark()
        self._generate_capitals_mark()
        self._save_data(f"locations_{self.geocoding_api}.csv")
