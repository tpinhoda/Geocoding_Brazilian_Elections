import logging
from dataclasses import dataclass
from src.election import Election
from urllib.request import urlretrieve
from os.path import join

@dataclass
class Raw(Election):
    """Object containing information to generate raw election data
    Attributes:
        url: The url to collect the raw data
    """

    url: str
   
    def _fill_url(self) -> str:
        """Fill the gaps in the election data url link"""
        return self.url.format(self.year, self.round)

    def _download_raw_data(self) -> None:
        """Donwload raw election data"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Downloading raw data.")
        name_file = 'polling_places.csv'
        urlretrieve(self.url, join(self.cur_dir, name_file))

    def generate_raw_data(self) -> None:
        """Generate election raw data"""
        self._initialize_folders()
        self._download_raw_data()
