"""Generates raw data for locations."""
from dataclasses import dataclass
from os.path import join
from urllib.request import urlretrieve
import zipfile
from src.election import Election


@dataclass(repr=True)
class Raw(Election):
    """Represents the Brazilian polling places in raw state of processing.

    This object collects and organize the Brazilian polling places.

    Attributes
    ---------
        url_data: str
            The url to collect the raw data.
        url_meshblock: str
            The utlo to collect the cities meshblock data.
        data_filename: str
            File identifying name.
        meshblock_filename: str
            Meshblock file identifying name.
    """

    url_data: str = None
    url_meshblock: str = None
    data_filename: str = None
    meshblock_filename: str = None

    def init_logger_name(self):
        """Initialize the logger name"""
        self.logger_name = "Locations (Raw)"

    def init_state(self):
        """Initialize the  process state name"""
        self.state = "raw"

    def _make_folders(self):
        """Make the initial folders"""
        self._make_initial_folders()
        self._mkdir(self.data_name)

    # Get locations file
    def _fill_url(self) -> str:
        """Fill the gaps in the election data url link"""
        return self.url_data.format(self.year, self.round)

    def _download_location_raw_data(self) -> None:
        """Donwload raw election data"""
        self.logger_info("Downloading Location raw data.")
        urlretrieve(self.url_data, join(self.cur_dir, self.data_filename))

    # Get meshblock files
    def _download_city_meshblock_data(self) -> None:
        """Donwload raw election data"""
        self._mkdir(self.meshblock_filename.split(".")[0])
        self.logger_info("Downloading city meshblock data.")
        urlretrieve(self.url_meshblock, join(self.cur_dir, self.meshblock_filename))

    def _unzip_city_meshblock_data(self) -> None:
        """Unzip only the csv raw data in the current directory"""
        self.logger_info("Unzipping city meshblock file.")
        with zipfile.ZipFile(
            join(self.cur_dir, self.meshblock_filename), "r"
        ) as zip_ref:
            zip_ref.extractall(self.cur_dir)

    def _rename_meshblock_files(self):
        """Rename cities meshblocks files"""
        self.logger_info("Renaming zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            old_name = filename
            new_name = (
                f"{self.meshblock_filename.split('.')[0]}.{filename.split('.')[1]}"
            )
            self._rename_file_from_cur_dir(old_name, new_name)

    def _remove_city_meshblock_zip_files(self) -> None:
        """Remove all zip files in the current directory"""
        self.logger_info("Removing zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            if filename.endswith(".zip"):
                self._remove_file_from_cur_dir(filename=filename)

    def _get_city_meshblock_file(self):
        """Donwload and unzip cities meshblocks"""
        self._download_city_meshblock_data()
        self._unzip_city_meshblock_data()
        self._rename_meshblock_files()
        self._remove_city_meshblock_zip_files()

    def _empty_folder_run(self):
        """Run without files in the folder"""
        self._download_location_raw_data()
        self._get_city_meshblock_file()

    def run(self) -> None:
        """Generate election raw data"""
        self.init_logger_name()
        self.init_state()
        self.logger_info("Generating raw data.")
        self._make_folders()
        files_exist = self._get_files_in_cur_dir()
        if not files_exist:
            self._empty_folder_run()
        else:
            self.logger_warning(
                "Non empty directory, the process only runs on empty folders!"
            )
