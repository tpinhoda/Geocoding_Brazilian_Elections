from dataclasses import dataclass
from src.abstract_data import Data


@dataclass
class Election(Data):
    """Object containg the information about election data
    Attributes:
        year        : Election year
        round       : Election round
        data_type   : Type of the data {results, polling_place}
    """

    year: str
    round: str
    data_type: str

    def _make_election_folders(self) -> None:
        """Create the election folds considering the year and round"""
        self.create_folder(self.year)
        self.create_folder("round_" + self.round)

    def _make_data_type_folder(self) -> None:
        """Create the type of data fold {raw, interim, processed}"""
        self.create_folder(self.data_type)

    def _initialize_folders(self) -> None:
        """Create all initial folds concerning the election data"""
        self._make_initial_folders()
        self._make_election_folders()
        self._make_data_state_folder()
        self._make_data_type_folder()
