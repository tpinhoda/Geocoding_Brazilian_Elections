"""Pipeline to process election data"""
from dataclasses import dataclass, field
from typing import Dict, Final, List
import inspect
from src.election import Election
from src.results.raw import Raw as ResultsRaw
from src.results.interim import Interim as ResultsInterim
from src.results.processed import Processed as ResultsProcessed
from src.locations.raw import Raw as LocationsRaw
from src.locations.interim import Interim as LocationsInterim
from src.locations.processed import Processed as LocationsProcessed


DATA_PROCESS_MAP: Final = {
    "results": {
        "raw": ResultsRaw,
        "interim": ResultsInterim,
        "processed": ResultsProcessed,
    },
    "locations": {
        "raw": LocationsRaw,
        "interim": LocationsInterim,
        "processed": LocationsProcessed,
    },
}


@dataclass
class Pipeline:
    """Represents a pipeline to process data.

    This object process Brazilian electoral data.

    Attributes
    ----------
    data_name: str
        Describes the type of data [location or results]
    params: Dict[str, str]
        Dictionary of parameters
    switchers: Dict[str, int]
        Dictionary of switchers to generate the pipeline
    """
    data_name: str
    params: Dict[str, str] = field(default_factory=dict)
    switchers: Dict[str, str] = field(default_factory=dict)
    __pipeline: List[str] = field(default_factory=list)
    __raw: Election = None
    __interim: Election = None
    __processed: Election = None

    @staticmethod
    def _get_class_attributes(class_process):
        """Returns the attributes required to instanciate a class"""
        attributes = inspect.getmembers(class_process, lambda a: not inspect.isroutine(a))
        return [
            a[0]
            for a in attributes
            if not (a[0].startswith("__") and a[0].endswith("__"))
        ]

    def _get_parameter_value(self, type_data, attributes):
        """Get parameter values"""
        parameters = {attr: self.params[type_data].get(attr) for attr in attributes}
        return {key: value for key, value in parameters.items() if value}

    def _generate_parameters(self, process):
        """Generate parameters dict"""
        attributes = self._get_class_attributes(process)
        global_parameters = self._get_parameter_value("global", attributes)
        process_parameters = self._get_parameter_value(self.data_name, attributes)
        return dict(global_parameters, **process_parameters)

    def _get_init_function(self, process):
        """Return the initialization fucntion"""
        return DATA_PROCESS_MAP[self.data_name][process]

    def init_raw(self):  # sourcery skip: class-extract-method
        """Initialize raw class"""
        data_class = self._get_init_function("raw")
        parameters = self._generate_parameters(data_class())
        self.__raw = data_class(**parameters)
        return self.__raw

    def init_interim(self):
        """Initialize interim class"""
        data_class = self._get_init_function("interim")
        parameters = self._generate_parameters(data_class())
        self.__interim = data_class(**parameters)
        return self.__interim

    def init_processed(self):
        """Initialize processed class"""
        data_class = self._get_init_function("processed")
        parameters = self._generate_parameters(data_class())
        self.__processed = data_class(**parameters)
        return self.__processed

    def get_pipeline_order(self):
        """Return pipeline order"""
        return [process for process in self.switchers if self.switchers[process]]

    def map_data_process(self, process):
        """Map the process initialization functions"""
        processes = {
            "raw": self.init_raw,
            "interim": self.init_interim,
            "processed": self.init_processed,
        }
        return processes[process]()

    def generate_pipeline(self):
        """Generate pipeline to process data"""
        pipeline_order = self.get_pipeline_order()
        for process in pipeline_order:
            self.__pipeline.append(self.map_data_process(process))

    def run(self):
        """Run pipeline"""
        self.generate_pipeline()
        for process in self.__pipeline:
            process.run()
