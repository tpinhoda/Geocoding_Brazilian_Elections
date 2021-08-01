import coloredlogs, logging
import re
import zipfile
from dataclasses import dataclass, field
from src.election import Election
from typing import List
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from os.path import join
from tqdm import tqdm


@dataclass
class Raw(Election):
    """Object containing information to generate raw election data
    Attributes:
        url: The url to collect the raw data
        html: the html page where the raw data can be downloaded
        links: list of links to download raw data
    """

    url: str = None
    html: str = None
    links: List[str] = field(default_factory=list)

    def _fill_url(self) -> str:
        """Fill the gaps in the election data url link"""
        return self.url.format(self.year, self.round)

    def _download_html(self) -> None:
        """Donwload the election results page"""
        self.url = self._fill_url()
        self.html = urlopen(self.url).read().decode("utf-8")

    def _get_links(self) -> None:
        """Get the links from the html"""
        soup = BeautifulSoup(self.html, "html.parser")
        for link in soup.findAll("a", attrs={"href": re.compile("\.zip$")}):
            self.links.append(link.get("href"))

    def _download_raw_data(self) -> None:
        """Donwload raw election data"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Downloading raw data.")
        for link in tqdm(self.links, desc="Downloading"):
            name_file = link.split("/")[-1]
            urlretrieve(link, join(self.cur_dir, name_file))

    def _unzip_raw_data(self) -> None:  # sourcery skip: class-extract-method
        """Unzip only the csv raw data in the current directory"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Unzipping raw data.")
        list_filename = self._get_files_in_cur_dir()
        for zip_filename in tqdm(list_filename, desc="Unziping"):
            with zipfile.ZipFile(join(self.cur_dir, zip_filename), "r") as zip_ref:
                for filename in zip_ref.namelist():
                    if filename.endswith(".csv"):
                        zip_ref.extract(filename, self.cur_dir)

    def _remove_zip_files(self) -> None:
        """Remove all zip files in the current directory"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Removing zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            if filename.endswith(".zip"):
                self._remove_file_from_cur_dir(filename=filename)

    def _rename_raw_data(self) -> None:
        """Rename all files in the current directory"""
        logger = logging.getLogger(self.logger_name)
        logger.info("Renameing csv files.")
        list_filename = self._get_files_in_cur_dir()
        for old_filename in list_filename:
            new_filename = old_filename.split("_")[2] + ".csv"
            self._rename_file_from_cur_dir(
                old_filename=old_filename, new_filename=new_filename
            )

    def generate_raw_data(self) -> None:
        """Generate election raw data"""
        self._initialize_folders()
        self._download_html()
        self._get_links()
        self._download_raw_data()
        self._unzip_raw_data()
        self._remove_zip_files()
        self._rename_raw_data()
