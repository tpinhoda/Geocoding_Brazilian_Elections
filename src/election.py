# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from os import mkdir, listdir, remove, rename
from os.path import join, isfile
from typing import List


@dataclass
class Election:
    """Represents an election dataset
    
    This class is responsible to organize in folds the election datasets.
    Attributes:
        region      : Name of the region described by the dataset
        org         : Name of the orgarnization where the data was collected
        year        : Election year
        round       : Election round
        data_type   : Type of the data {results, polling_place}
        logger_name : Name of the looger to display in the logging
        root_path   : Root path
        cur_dir     : Current directory
        data_state  : Sate of the data {raw, interim, processed}
    """

    region: str
    org: str
    year: str
    round: str
    data_type: str
    logger_name: str
    root_path: str
    cur_dir: str
    data_state: str


    def _create_folder(self, folder_name: str) -> None:
        """Creates a folder at current path"""
        logger = logging.getLogger(self.logger_name)
        self.cur_dir = join(self.cur_dir, folder_name)
        try:
            mkdir(self.cur_dir)
            logger.info(f"Creating folder: /{folder_name}")
        except FileExistsError:
            logger.info(f"Entering folder: /{folder_name}")

    def _get_initial_folders_path(self) -> str:
        """Returns the initial folders path"""
        return join(self.root_path, self.region, self.org)
    
    def _get_election_folders_path(self) -> str:
        """Returns the election folders path"""
        return join(self.root_path, self.region, self.org, 
                    self.year, 'round_'+ self.round )
    
    def _get_state_folders_path(self, state: str) -> str:
        """Returns the data state path"""
        return join(self.root_path, self.region, self.org, 
                    self.year, 'round_'+ self.round, state)

    def _get_files_in_cur_dir(self) -> List[str]:
        """Returns a list of filesnames in the current directory"""
        return [
            filename
            for filename in listdir(self.cur_dir)
            if isfile(join(self.cur_dir, filename))
        ]

    def _remove_file_from_cur_dir(self, filename: str) -> None:
        """Remvoves a filename from the current directory"""
        remove(join(self.cur_dir, filename))

    def _rename_file_from_cur_dir(self, old_filename: str, new_filename: str) -> None:
        """Rename a file from the current dir"""
        rename(join(self.cur_dir, old_filename), join(self.cur_dir, new_filename))
    
    def _make_initial_folders(self) -> None:
        """Creates the initial folds to store the dataset"""
        logger = logging.getLogger(self.logger_name)
        logger.info(f"Root: {self.root_path}")
        self._create_folder(folder_name=self.region)
        self._create_folder(folder_name=self.org)
    
    def _make_election_folders(self) -> None:
        """Create the election folds considering the year and round"""
        self._create_folder(self.year)
        self._create_folder("round_" + self.round)

    def _make_data_state_folder(self) -> None:
        """Create folder regarding the data state {raw, interim, processed}"""
        self._create_folder(folder_name=self.data_state)

    def _make_data_type_folder(self) -> None:
        """Create the type of data fold {raw, interim, processed}"""
        self._create_folder(self.data_type)

    def _initialize_folders(self) -> None:
        """Create all initial folds concerning the election data"""
        self._make_initial_folders()
        self._make_election_folders()
        self._make_data_state_folder()
        self._make_data_type_folder()


