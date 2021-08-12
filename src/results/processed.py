"""Generates processed data regarding election results"""
import json
from os.path import join
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandas as pd
from src.election import Election


@dataclass
class Processed(Election):
    """Represents the Brazilian election results in processed state of processing.

    This object pre-processes the Brazilian election results.

    Attributes
    ----------
        aggregation_level: str
            The data geogrephical level of aggrevation
        candidacy_pos: str
            The candidacy position [presidente, governador]
        geocoding_api: str
            The geocoding api to be used (Google Maps: GMAPS, OpenStreep Map: OSM)
        levenshtesin_threshold: float
            The threshold considereing the levenshtein similarity measure of addresses
        precision_filter: List[str]
            The list of precision to be filter from the dataset
        city_limits_fitler: List[str]
            The list of city limits to be filter from the dataset
    """

    aggregation_level: str = None
    candidacy_pos: str = None
    geocoding_api: str = None
    candidates: str = None
    levenshtein_threshold: float = None
    precision_filter: List[str] = field(default_factory=list)
    city_limits_filter: List[str] = field(default_factory=list)
    __data: pd.DataFrame = field(default_factory=pd.DataFrame)
    __data_info: Dict = field(default_factory=dict)
    __per: Optional[int] = None

    def _read_data_csv(self) -> pd.DataFrame:
        """Read the data.csv file and returns a pandas dataframe"""
        self.logger_info("Reading interim data.")
        filepath = join(
            self._get_process_folder_path(state="interim"),
            self.data_name,
            self.aggregation_level,
            self.candidacy_pos,
            f"data_{self.geocoding_api}.csv",
        )
        self.__data = pd.read_csv(filepath).infer_objects()
    
    def _remove_external_places(self) -> pd.DataFrame:
        self.__data = self.__data[self.__data["[GEO]_UF"] != "ZZ"]

    def _get_candidates_cols(self):
        """Returns the candidates columns"""
        return [col for col in self.__data if "CANDIDATE" in col]

    def _get_data_info(self):
        """Returns the interim data basic information"""
        candidate_cols = self._get_candidates_cols()
        self.__data_info["size"] = len(self.__data)
        self.__data_info["turnout"] = self.__data["[ELECTION]_TURNOUT"].sum()
        self.__data_info["candidates_votes"] = {
            col: self.__data[col].sum() for col in candidate_cols
        }
        self.__data_info["null_votes"] = self.__data["[ELECTION]_NULL"]
        self.__data_info["null_blank"] = self.__data["[ELECTION]_BLANK"]

    def _filter_data(self):
        """Filter the dataset according to parameters"""
        self.logger_info("Filtering dataset by parameters.")
        self.__data = self.__data[
            self.__data["[GEO]_LEVENSHTEIN_SIMILARITY"] >= float(self.levenshtein_threshold)
        ]
        self.__data = self.__data[
            self.__data["[GEO]_CITY_LIMITS"].isin(self.city_limits_filter)
        ]
        self.__data = self.__data[
            self.__data["[GEO]_PRECISION"].isin(self.precision_filter)
        ]

    def _calculate_per(self) -> int:
        """Calculates the dataset's percentual of electorate representation"""
        original_turnout = self.__data_info["turnout"]
        filtered_turnout = self.__data["[ELECTION]_TURNOUT"].sum()
        self.__per = 100 * filtered_turnout / original_turnout

    def _make_per_fold(self):
        """Make the per folder"""
        self._mkdir("PER_{:.2f}".format(self.__per))

    def _save_data(self):
        """Save the dataset"""
        self.logger_info("Saving final dataset.")
        self.__data.to_csv(
            join(self.cur_dir, f"data_{self.geocoding_api}.csv"), index=False
        )

    def _generate_report(self):
        """Generates json report concerning the parameters used to create the dataset"""
        self.logger_info("Generating final report.")
        report_dict = {
            "Levenshtein Threshold": str(self.levenshtein_threshold),
            "City Limits": self.city_limits_filter,
            "Precisions": self.precision_filter,
            "Candidates": self.candidates,
            "#Rows": f"{len(self.__data)} ({100 * len(self.__data) / self.__data_info['size']}%)",
        }

        with open(join(self.cur_dir, "parameters.json"), "w") as file:
            json.dump(report_dict, file, indent=4)

    def run(self):
        """Run process"""
        self.init_logger_name(msg="Results (Processed)")
        self.init_state(state="processed")
        self.logger_info("Generating processed data.")
        self._make_folders(
            folders=[self.data_name, self.aggregation_level, self.candidacy_pos.lower()]
        )
        self._read_data_csv()
        self._remove_external_places()
        self._get_data_info()
        self._filter_data()
        self._calculate_per()
        self._make_per_fold()
        self._save_data()
        self._generate_report()
