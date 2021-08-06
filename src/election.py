# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from os import mkdir, listdir, remove, rename
from os.path import join, isfile
from abc import ABC, abstractmethod
from typing import List


@dataclass
class Election(ABC):
    """Represents an Brazilian election.

    This class is responsible to organize in folders the election datasets.

    Attributes
    ----------
        region : str
            Name of the region described by the dataset
        org  : str
            Name of the orgarnization where the data was collected
        year : str
            Election year
        round : str
            Election round
        root_path : str
            Root path
        cur_dir: str
            Currenti working directory
        logger_name: str
            Name of the logger

    """

    region: str = None
    org: str = None
    year: str = None
    round: str = None
    root_path: str = None
    data_name: str = None
    cur_dir: str = None
    logger_name: str = None
    state: str = None

    def logger_info(self, message: str):
        """Print longger info message"""
        logger = logging.getLogger(self.logger_name)
        logger.info(message)

    def _mkdir(self, folder_name: str) -> None:
        """Creates a folder at current path"""
        # logger = logging.getLogger(self.logger_name)
        self.cur_dir = join(self.cur_dir, folder_name)
        try:
            mkdir(self.cur_dir)
            # logger.info(f"Creating folder: /{folder_name}")
        except FileExistsError:
            pass
            # logger.info(f"Entering folder: /{folder_name}")

    def _make_initial_folders(self) -> None:
        """Creates the initial folds to store the dataset"""
        self.logger_info(f"Root: {self.root_path}")
        self.logger_info("Creating or Entering dataset folders.")
        self.cur_dir = self.root_path
        self._mkdir(folder_name=self.region)
        self._mkdir(folder_name=self.org)
        self._mkdir(folder_name=self.year)
        self._mkdir(folder_name=f"round_{self.round}")
        self._mkdir(folder_name=self.state)

    def _get_process_folder_path(self, state: str) -> str:
        """Returns the data state path"""
        return join(
            self.root_path,
            self.region,
            self.org,
            self.year,
            "round_" + self.round,
            state,
        )

    def _get_initial_folders_path(self) -> str:
        """Returns the initial folders path"""
        return join(self.root_path, self.region, self.org)

    def _get_election_folders_path(self) -> str:
        """Returns the election folders path"""
        return join(
            self.root_path, self.region, self.org, self.year, "round_" + self.round
        )

    def _get_state_folders_path(self, state: str) -> str:
        """Returns the data state path"""
        return join(
            self.root_path,
            self.region,
            self.org,
            self.year,
            "round_" + self.round,
            state,
        )

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
        try:
            rename(join(self.cur_dir, old_filename), join(self.cur_dir, new_filename))
        except FileExistsError:
            pass

    @abstractmethod
    def init_logger_name(self):
        pass

    @abstractmethod
    def init_state(self):
        pass

    @abstractmethod
    def _make_folders(self):
        pass

    @abstractmethod
    def run(self):
        pass
