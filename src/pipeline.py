from dataclasses import dataclass, field
from src.election import Election
from typing import Dict, List
from src.results.raw import Raw as ResultsRaw
from src.results.interim import Interim as ResultsInterim
from src.results.processed import Processed as ResultsProcessed
from src.locations.raw import Raw as LocationsRaw
from src.locations.interim import Interim as LocationsInterim
from src.locations.processed import Processed as LocationsProcessed


@dataclass
class Pipeline:
    data_name: str
    params: Dict[str, str] = field(default_factory=dict)
    switchers: Dict[str, str] = field(default_factory=dict)
    __pipeline: List[str] = field(default_factory=list)
    __raw: Election = None
    __interim: Election = None
    __processed: Election = None

    def _generate_parameters(self, process):
        parameters = dict(self.params["global"], **self.params[self.data_name][process])
        parameters["data_name"] = self.params[self.data_name]["data_name"]
        if process == "interim":
            parameters["data_filename"] = self.params[self.data_name]["raw"][
                "data_filename"
            ]
            parameters["meshblock_filename"] = self.params[self.data_name]["raw"]["meshblock_filename"]
        elif process == "processed":
            parameters["meshblock_filename"] = self.params[self.data_name]["raw"]["meshblock_filename"]
            parameters["geocoding_api"] = self.params[self.data_name]["interim"]["geocoding_api"]
            parameters["aggregation_level"] = self.params[self.data_name]["interim"]["aggregation_level"]
        return parameters

    def init_location_raw(self, process: str):
        parameters = self._generate_parameters(process)
        self.__raw = LocationsRaw(**parameters)
        return self.__raw

    def init_location_interim(self, process: str):
        parameters = self._generate_parameters(process)
        self.__interim = LocationsInterim(**parameters)
        return self.__interim

    def init_location_processed(self, process: str):
        parameters = self._generate_parameters(process)
        self.__processed = LocationsProcessed(**parameters)
        return self.__processed

    def get_pipeline_order(self):
        return [process for process in self.switchers if self.switchers[process]]

    def map_data_process(self, process):
        processes = {
            "locations_raw": self.init_location_raw,
            "locations_interim": self.init_location_interim,
            "locations_processed": self.init_location_processed,
            "results_raw": None,
            "results_interim": None,
            "results_processed": None,
        }
        return processes[f"{self.data_name}_{process}"](process)

    def generate_pipeline(self):
        pipeline_order = self.get_pipeline_order()
        for process in pipeline_order:
            self.__pipeline.append(self.map_data_process(process))

    def run(self):
        self.generate_pipeline()
        for process in self.__pipeline:
            process.run()
