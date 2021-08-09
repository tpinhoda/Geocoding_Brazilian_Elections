from os.path import join
from typing import Dict, List
from dataclasses import dataclass, field
import re
from src.locations.interim import MAP_COL_RENAME
from tqdm import tqdm
import pandas as pd
from pandas.api.types import is_numeric_dtype
from src.election import Election


MAP_CANDIDACY = {"presidente": 1, "governador": 3}

MAP_COL_RENAME = {
    "SG_ UF": "[GEO]_UF",
    "CD_MUNICIPIO": "[GEO]_ID_TSE_CITY",
    "NM_MUNICIPIO": "[GEO]_CITY",
    "NR_ZONA": "[GEO]_ID_POLLING_ZONE",
    "NR_SECAO": "[GEO]_ID_POLLING_SECTION",
    "NR_LOCAL_VOTACAO": "[GEO]_ID_POLLING_PLACE",
    "CD_CARGO_PERGUNTA": "[ELECTION]_ID_CANDIDACY_POSITION",
    "QT_APTOS": "[ELECTION]_ELECTORATE",
    "QT_COMPARECIMENTO": "[ELECTION]_TURNOUT",
    "QT_ABSTENCOES": "[ELECTION]_ABSTENTIONS",
    "NR_VOTAVEL": "[ELECTION]_ID_CANDIDATE",
    "QT_VOTOS": "[ELECTION]_VOTES",
    "QT_ELEITORES_BIOMETRIA_NH": "[ELECTION]_ELECTORATE_BIOMETRIA",
}


@dataclass
class Interim(Election):
    candidacy_pos: str = None
    aggregation_level: str = None
    __results_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __locations_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __list_results_data: List[pd.DataFrame] = field(default_factory=list)

    def init_logger_name(self):
        """Initialize the logger name"""
        self.logger_name = "Results (Interim)"

    def init_state(self):
        """Initialize the  process state name"""
        self.state = "interim"

    def _make_folders(self):
        """Make the initial folders"""
        self._make_initial_folders()
        self._mkdir(self.data_name)
        self._mkdir(self.candidacy_pos.lower())

    def _read_results_csv(self, data_filename) -> pd.DataFrame:
        """Read the polling places.csv file and returns a pandas dataframe"""
        filepath = join(
            self._get_process_folder_path(state="raw"),
            self.data_name,
            data_filename,
        )
        self.__results_data = pd.read_csv(
            filepath,
            sep=";",
            encoding="latin1",
            na_values=["#NULO#", -1, -3],
            low_memory=False,
        ).infer_objects()
    
    def _read_locations_csv(self) -> pd.DataFrame:
        filepath = join(
            self._get_process_folder_path(state="processed"),
            "locations",
            self.aggregation_level,
            "locations_IBGE.csv",
        )
        self.__locations_data = pd.read_csv(filepath).infer_objects()
        

    def _rename_cols(self) -> pd.DataFrame:
        """Rename columns"""
        self.__results_data.rename(columns=MAP_COL_RENAME, inplace=True)
        self.__results_data = self.__results_data[MAP_COL_RENAME.values()]

    def _filter_by_candidacy_pos(self):
        """Filter election results by candidacy position"""
        self.__results_data = self.__results_data[
            self.__results_data["[ELECTION]_ID_CANDIDACY_POSITION"]
            == MAP_CANDIDACY[self.candidacy_pos]
        ]

    def _get_votes_by_candidates(self) -> pd.DataFrame:
        """Get votes by candadidate"""
        return (
            self.__results_data.copy()
            .set_index(
                [
                    "[GEO]_ID_TSE_CITY",
                    "[GEO]_ID_POLLING_ZONE",
                    "[GEO]_ID_POLLING_PLACE",
                    "[GEO]_ID_POLLING_SECTION",
                    "[ELECTION]_ID_CANDIDATE",
                ]
            )
            .unstack(fill_value=0)["[ELECTION]_VOTES"]
        )

    def _drop_duplicated_rows(self) -> pd.DataFrame:
        self.__results_data.drop_duplicates(
            subset=[
                "[GEO]_ID_TSE_CITY",
                "[GEO]_ID_POLLING_ZONE",
                "[GEO]_ID_POLLING_PLACE",
                "[GEO]_ID_POLLING_SECTION",
            ],
            inplace=True
        )

    def _join_votes(self, votes: pd.DataFrame) -> pd.DataFrame:
        # Index data
        self.__results_data.set_index(
            keys=[
                "[GEO]_ID_TSE_CITY",
                "[GEO]_ID_POLLING_ZONE",
                "[GEO]_ID_POLLING_PLACE",
                "[GEO]_ID_POLLING_SECTION",
            ],
            inplace=True,
        )
        # Join votes and data
        self.__results_data = self.__results_data.join(votes)
        self.__results_data.reset_index(inplace=True)
        self.__results_data.drop("[ELECTION]_VOTES", axis=1, inplace=True)
    
    def _drop_na_cols(self) -> pd.DataFrame:
        self.__results_data.dropna(axis=1, inplace=True)
    
    def _drop_na_candidates(self) -> pd.DataFrame:
        self.__results_data.dropna(subset=["[ELECTION]_ID_CANDIDATE"], axis=0, inplace=True)
    
    def _fill_na_electorate_biometry(self) -> pd.DataFrame:
        self.__results_data["[ELECTION]_ELECTORATE_BIOMETRIA"].fillna(0, inplace=True)
    
    def _convert_cols_to_str(self) -> pd.DataFrame:
        self.__results_data.columns = self.__results_data.columns.astype(str)
        
    def _structure_data(self):
        # List raw data
        raw_dir = join(self._get_state_folders_path(state="raw"), self.data_name)
        filenames = self._get_files_in_id(raw_dir)
        for filename in tqdm(filenames, desc="Pre-Processing", leave=False):
            # Load raw data
            self._read_results_csv(filename)
            self._rename_cols()
            self._filter_by_candidacy_pos()
            self._drop_na_candidates()
            self._fill_na_electorate_biometry()
            votes = self._get_votes_by_candidates()
            self._drop_duplicated_rows()
            self._join_votes(votes)
            self._drop_na_cols()
            # Save the data as csv
            self.__list_results_data.append(self.__results_data.copy())
    
    def _concatenate_list_results_data(self) -> pd.DataFrame:
        self.__results_data = pd.concat(self.__list_results_data)
        self._convert_cols_to_str()
        self.__list_results_data = []
    
    def _create_aggregation_map(self) -> Dict[str, str]:
        return {col: ("sum" if is_numeric_dtype(self.__results_data[col]) and "ID" not in col else "first") for col in self.__results_data.columns}

    @staticmethod
    def _concat_cols(row):
        return "".join(row)
    
    def _get_merging_keys(self) -> List[str]:
        merging_keys = {"polling_place": ["[GEO]_UF", "[GEO]_CITY", "[GEO]_POLLING_ZONE", ["GEO_POLLING_PLACE"]],
                        "city": ["[GEO]_UF", "[GEO]_CITY"]}
        return merging_keys[self.aggregation_level]
    
    def _merge_results_and_location_data(self):
        self.logger_info("Merging results and location data.")
        # Load data with geocode information from polling places
        self._read_locations_csv()
        # Generate an id to facilitate merging
        merging_keys = self._get_merging_keys()
        self.__results_data = self.__results_data.merge(self.__locations_data, on=merging_keys, how=left)
        print(self.__results_data)
        exit()
        polling_places["id_polling_place"] = (
            polling_places["SGL_UF"]
            + polling_places["LOCALIDADE_LOCAL_VOTACAO"]
            + polling_places["ZONA"].astype(str)
            + polling_places["NUM_LOCAL"].astype(str)
        )

        # List interim data
        filenames = [
            filename
            for filename in listdir(election_results)
            if isfile(join(election_results, filename))
        ]
        list_state_df = []
        # Merge each file in filenames
        for filename in tqdm(filenames):
            # Load interim data
            filepath = election_results + filename
            results = pd.read_csv(filepath)
            # Delete interim file
            remove(filepath)
            # Convert columns to string type
            results["NR_ZONA"] = results["NR_ZONA"].astype(str)
            results["NR_LOCAL_VOTACAO"] = results["NR_LOCAL_VOTACAO"].astype(str)
            results["NR_SECAO"] = results["NR_SECAO"].astype(str)
            # Generate an ids to facilitate merging
            id_pl = (
                results["SG_ UF"]
                + results["NM_MUNICIPIO"]
                + results["NR_ZONA"]
                + results["NR_LOCAL_VOTACAO"]
            )
            id_sec = (
                results["SG_ UF"]
                + results["NM_MUNICIPIO"]
                + results["NR_ZONA"]
                + results["NR_SECAO"]
            )
            id_zone = results["SG_ UF"] + results["NM_MUNICIPIO"] + results["NR_ZONA"]
            id_city = results["SG_ UF"] + results["NM_MUNICIPIO"]
            # Input columns ids
            results["id_polling_place"] = id_pl
            results["id_section"] = id_sec
            results["id_zone"] = id_zone
            results["id_city"] = id_city
            # Merge datasets
            merged = results.merge(
                polling_places[
                    [
                        "id_polling_place",
                        "COD_LOCALIDADE_IBGE",
                        "local_unico",
                        "lat",
                        "lon",
                        "geometry",
                        "rural",
                        "capital",
                        "precision",
                        "lev_dist",
                        "city_limits",
                    ]
                ],
                on="id_polling_place",
                how="left",
            )
            # Save the data as csv (Does not make sense have redundant data)
            # merged.to_csv(states_output_filepath + filename, index=False)

            # Append to list
            list_state_df.append(merged)

        # Save final dataset
        output_filepath = output_filepath + "data.csv"
        logger.info("2 - Saving final dataset in:\n" + output_filepath)
        final_df = pd.concat(list_state_df)
        final_df.to_csv(output_filepath, index=False)
        logger.info("Done!")
        return final_df

    def run(self):
        """Run process"""
        self.init_logger_name()
        self.init_state()
        self.logger_info("Generating interim data.")
        self._make_folders()
        self._structure_data()
        self._concatenate_list_results_data()
        print(self._create_aggregation_map())
        exit()
        #self._merge_results_and_location_data()
        print(self.__results_data.columns.values)
