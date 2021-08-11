"""Generates interim data for locations."""
from os.path import join
from dataclasses import dataclass, field
from typing import Optional
import pandas as pd
import geopandas as gpd
import numpy as np
import googlemaps
from tqdm import tqdm
from geopy.geocoders import Nominatim
from src.election import Election

MAP_COL_DTYPES = {
    "SGL_UF": "str",
    "COD_LOCALIDADE_IBGE": "str",
    "LOCALIDADE_LOCAL_VOTACAO": "str",
    "ZONA": "str",
    "BAIRRO_ZONA_SEDE": "str",
    "LATITUDE_ZONA": "float",
    "LONGITUDE_ZONA": "float",
    "NUM_LOCAL": "str",
    "SITUACAO_LOCAL": "str",
    "TIPO_LOCAL": "str",
    "LOCAL_VOTACAO": "str",
    "ENDERECO": "str",
    "BAIRRO_LOCAL_VOT": "str",
    "CEP": "str",
    "LATITUDE_LOCAL": "float",
    "LONGITUDE_LOCAL": "float",
    "NUM_SECAO": "str",
    "SECAO_AGREGADORA": "str",
    "SECAO_AGREGADA": "str",
}

MAP_COL_RENAME = {
    "SGL_UF": "[GEO]_UF",
    "COD_LOCALIDADE_IBGE": "[GEO]_ID_IBGE_CITY",
    "LOCALIDADE_LOCAL_VOTACAO": "[GEO]_CITY",
    "ZONA": "[GEO]_ID_POLLING_ZONE",
    "BAIRRO_ZONA_SEDE": "[GEO]_POLLING_ZONE",
    "NUM_LOCAL": "[GEO]_ID_POLLING_PLACE",
    "LOCAL_VOTACAO": "[GEO]_POLLING_PLACE",
    "BAIRRO_LOCAL_VOT": "[GEO]_POLLING_PLACE_NEIGHBORHOOD",
    "ENDERECO": "[GEO]_POLLING_PLACE_ADDRESS",
    "CEP": "[GEO]_CEP_CODE",
    "LATITUDE_LOCAL": "[GEO]_LATITUDE",
    "LONGITUDE_LOCAL": "[GEO]_LONGITUDE",
}


@dataclass
class Interim(Election):
    """Represents the Brazilian polling places in interim state of processing.

    This object pre-processes and geocodes the Brazilian polling places.

    Attributes
    ----------
        aggregation_level: str
            The level of aggregation [polling_places:, neighborhood, city]
        geocoding_api: str
            The geocoding api to be used (Google Maps: GMAPS, OpenStreep Map: OSM)
        api_key: Optional[str]
            The key for api that need key
        save_at: int = 1000
            The interval of addresses to save the polling_places file
    """

    aggregation_level: str = None
    geocoding_api: str = None
    api_key: Optional[str] = field(default_factory=str)
    meshblock_filename: Optional[str] = field(default_factory=str)
    meshblock_crs: str = None
    meshblock_col_id: Optional[str] = field(default_factory=str)
    save_at: int = 10
    data_filename: str = None
    __data: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Pre-Processing functions
    def _read_csv(self):
        """Read the polling places.csv file and returns a pandas dataframe"""
        self.logger_info("Reading raw data.")
        filepath = join(
            self._get_process_folder_path(state="raw"),
            self.data_name,
            self.data_filename,
        )
        self.__data = pd.read_csv(
            filepath, encoding="Latin5", sep=";", decimal=",", dtype=MAP_COL_DTYPES
        ).infer_objects()

    def _rename_cols(self):
        """Filter and rename only relevant columns"""
        self.__data.rename(columns=MAP_COL_RENAME, inplace=True)
        self.__data = self.__data[MAP_COL_RENAME.values()]

    def _clean_addresses(self):
        """Remove unecessary information from addrresses"""
        self.__data["[GEO]_CLEAN_ADDRESS"] = self.__data[
            "[GEO]_POLLING_PLACE_ADDRESS"
        ].str.replace(" - ZONA URBANA", "")
        self.__data["[GEO]_CLEAN_ADDRESS"] = self.__data[
            "[GEO]_POLLING_PLACE_ADDRESS"
        ].str.replace(" - ZONA RURAL", "")

    @staticmethod
    def _concat_cols(row):
        return "".join(row)

    def _aggregate_data(self):
        """Generate a unique id for each polling place"""
        id_template = {
            "polling_places": [
                "[GEO]_ID_POLLING_PLACE",
                "[GEO]_POLLING_ZONE",
                "[GEO]_ID_IBGE_CITY",
                "[GEO]_UF",
            ],
            "neighborhood": ["[GEO]_POLLING_PLACE_NEIGHBORHOOD", "[GEO]_ID_IBGE_CITY"],
            "city": ["[GEO]_ID_IBGE_CITY"],
        }
        self.__data["ID"] = self.__data[id_template[self.aggregation_level]].apply(
            self._concat_cols, axis=1
        )
        self.__data = self.__data.groupby(by="ID").agg("first")

    def _remove_foreign_places(self):
        """Remove places that are out of the region"""
        self.__data = self.__data[self.__data["[GEO]_UF"] != "ZZ"]

    def _create_precision_attribute(self):
        """Create the precision attribute regarding the latitude and longitude"""
        self.__data["[GEO]_PRECISION"] = np.where(
            np.isnan(self.__data["[GEO]_LATITUDE"]), None, "TSE"
        )

    def _create_fetched_address_attribute(self):
        """Create the fetched address regarding the latitude and longitude"""
        self.__data["[GEO]_FETCHED_ADDRESS"] = np.where(
            self.__data["[GEO]_PRECISION"] != "TSE",
            None,
            self.__data["[GEO]_CLEAN_ADDRESS"],
        )

    def _create_query_address_attribute(self):
        """Create the query address regarding the latitude and longitude"""
        self.__data["[GEO]_QUERY_ADDRESS"] = np.where(
            self.__data["[GEO]_PRECISION"] != "TSE",
            None,
            self.__data["[GEO]_CLEAN_ADDRESS"],
        )

    def _save_data(self, filename):
        """save the __data in the iterim folder"""
        self.__data.to_csv(join(self.cur_dir, filename), index=False)

    def _preprocessing_data(self):
        """Pre-processing of the polling places"""
        self._read_csv()
        self.logger_info("Pre-Processing data.")
        self._rename_cols()
        self._clean_addresses()
        self._remove_foreign_places()
        self._aggregate_data()
        self._create_precision_attribute()
        self._create_fetched_address_attribute()
        self._create_query_address_attribute()

    def _generate_address(self, row):
        """Generate the address based on the aggregation level"""
        address_template = {
            "polling_places": [
                "[GEO]_POLLING_PLACE",
                "[GEO]_CLEAN_ADDRESS",
                "[GEO]_CITY",
                "[GEO]_UF",
            ],
            "neighborhood": [
                "[GEO]_POLLING_PLACE_NEIGHBORHOOD",
                "[GEO]_CITY",
                "[GEO]_UF",
            ],
            "city": ["[GEO]_CITY", "[GEO]_UF"],
        }
        return row[address_template[self.aggregation_level]].str.cat(sep=", ")

    # Geocoding functions
    def _googlemaps_geocoding(self):
        """Get coordinates for each polling place using google maps geocoding api"""
        gmaps = googlemaps.Client(key=self.api_key, queries_per_second=40)
        for count_rows, (index, row) in tqdm(
            enumerate(self.__data.iterrows()), total=len(self.__data), desc="Geocoding"
        ):
            if not row["[GEO]_PRECISION"]:
                try:
                    components = {
                        "country": self.region,
                        "administrative_area": row["[GEO]_CITY"],
                    }
                    address = self._generate_address(row)
                    self.__data.loc[index, "[GEO]_QUERY_ADDRESS"] = address
                    result = gmaps.geocode(
                        language="pt-BR", address=address, components=components
                    )
                    if not result:
                        continue
                    self.__data.loc[index, "[GEO]_LATITUDE"] = result[0]["geometry"][
                        "location"
                    ]["lat"]
                    self.__data.loc[index, "[GEO]_LONGITUDE"] = result[0]["geometry"][
                        "location"
                    ]["lng"]
                    self.__data.loc[index, "[GEO]_PRECISION"] = result[0]["geometry"][
                        "location_type"
                    ]
                    self.__data.loc[index, "[GEO]_FETCHED_ADDRESS"] = result[0][
                        "formatted_address"
                    ]
                except ConnectionError:
                    pass
            if not (count_rows + 1) % self.save_at:
                self._save_data("locations_GMAPS.csv")

    def _openstreet_geocoding(self):
        """Get coordinates for each polling place using openstreet map geocoding api"""
        geolocator = Nominatim(user_agent="brazilian_polling_places")
        for count_rows, (index, row) in tqdm(
            enumerate(self.__data.iterrows()), total=len(self.__data), desc="Geocoding"
        ):
            if not row["[GEO]_PRECISION"]:
                try:
                    address = self._generate_address(row)
                    self.__data.loc[index, "[GEO]_QUERY_ADDRESS"] = address
                    result = geolocator.geocode(address)
                    if not result:
                        continue
                    self.__data.loc[index, "[GEO]_LATITUDE"] = result.latitude
                    self.__data.loc[index, "[GEO]_LONGITUDE"] = result.longitude
                    self.__data.loc[index, "[GEO]_PRECISION"] = "OSM"
                    self.__data.loc[index, "[GEO]_FETCHED_ADDRESS"] = result.address

                except ConnectionError:
                    pass
            if not (count_rows + 1) % self.save_at:
                self._save_data("locations_OSM.csv")

    def _ibge_geocoding(self):
        filename = self.meshblock_filename.split(".")[0]
        meshblock_filepath = join(
            self._get_process_folder_path(state="raw"),
            self.data_name,
            filename,
            f"{filename}.shp",
        )
        meshblock = gpd.read_file(meshblock_filepath)
        meshblock["geometry"] = meshblock["geometry"].to_crs(crs=self.meshblock_crs)
        meshblock["Y"] = meshblock.to_crs("+proj=cea").centroid.to_crs(meshblock.crs).y
        meshblock["X"] = meshblock.to_crs("+proj=cea").centroid.to_crs(meshblock.crs).x

        self.__data = self.__data.merge(
            meshblock[["CD_GEOCMU", "X", "Y"]],
            left_on="[GEO]_ID_IBGE_CITY",
            right_on=self.meshblock_col_id,
            how="left",
        )
        self.__data["[GEO]_QUERY_ADDRESS"] = self.__data["[GEO]_CLEAN_ADDRESS"]
        self.__data["[GEO]_LONGITUDE"] = self.__data["X"]
        self.__data["[GEO]_LATITUDE"] = self.__data["Y"]
        self.__data["[GEO]_FETCHED_ADDRESS"] = self.__data["[GEO]_CLEAN_ADDRESS"]
        self.__data["[GEO]_PRECISION"] = self.geocoding_api
        self.__data.drop([self.meshblock_col_id, "X", "Y"], axis=1, inplace=True)

        self._save_data("locations_IBGE.csv")

    def _geocode_data(self):
        """Run geocode function depending on the api chosen."""
        self.logger_info(f"Geocoding with {self.geocoding_api}")
        api_func = {
            "GMAPS": self._googlemaps_geocoding,
            "OSM": self._openstreet_geocoding,
            "IBGE": self._ibge_geocoding,
        }
        return api_func.get(self.geocoding_api)()

    def run(self):
        """Generates interim __data regarding the polling places"""
        self.init_logger_name(msg="Locations (Interim)")
        self.init_state(state="interim")
        self.logger_info("Generating interim data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        self._preprocessing_data()
        self._geocode_data()
