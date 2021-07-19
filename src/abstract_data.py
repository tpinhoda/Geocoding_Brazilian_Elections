# -*- coding: utf-8 -*-
import logging
from dataclasses import dataclass
from os import mkdir, listdir, remove, rename
from os.path import join, isfile
from typing import List


@dataclass
class Data:
    """Object that contains information about the election dataset
    Attributes:
        region      : Name of the region described by the dataset
        logger_name : Name of the looger to display in the logging
        root_path   : Root path
        cur_dir     : Current directory
        data_state  : Sate of the data {raw, interim, processed}
        org         : Name of the orgarnization where the data was collected
    """

    region: str
    logger_name: str
    root_path: str
    cur_dir: str
    data_state: str
    org: str

    def create_folder(self, folder_name: str) -> None:
        """Creates a folder at current path"""
        logger = logging.getLogger(self.logger_name)
        self.cur_dir = join(self.cur_dir, folder_name)
        try:
            mkdir(self.cur_dir)
            logger.info(f"Creating folder: /{folder_name}")
        except FileExistsError:
            logger.info(f"Entering folder: /{folder_name}")

    def _make_initial_folders(self) -> None:
        """Creates the initial folds to store the dataset"""
        logger = logging.getLogger(self.logger_name)
        logger.info(f"Root: {self.root_path}")
        self.create_folder(folder_name=self.region)
        self.create_folder(folder_name=self.org)

    def _make_data_state_folder(self) -> None:
        """Create folder regarding the data state {raw, interim, processed}"""
        self.create_folder(folder_name=self.data_state)

    def get_initial_folders_path(self) -> str:
        """Returns the initial folders path"""
        return join(self.root_path, self.region, self.org)

    def get_files_in_cur_dir(self) -> List[str]:
        """Returns a list of filesnames in the current directory"""
        return [
            filename
            for filename in listdir(self.cur_dir)
            if isfile(join(self.cur_dir, filename))
        ]

    def remove_file_from_cur_dir(self, filename: str) -> None:
        """Remvoves a filename from the current directory"""
        remove(join(self.cur_dir, filename))

    def rename_file_from_cur_dir(self, old_filename: str, new_filename: str) -> None:
        """Rename a file from the current dir"""
        rename(join(self.cur_dir, old_filename), join(self.cur_dir, new_filename))
