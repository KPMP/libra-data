import os
import logging
import shutil
import filecmp
import hashlib
import uuid
from zarr_checksum import compute_zarr_checksum
from zarr_checksum.generators import yield_files_local

logger = logging.getLogger("DLUFilesystem")
logger.setLevel(logging.INFO)

DLU_PACKAGE_DIR_PREFIX = 'package_'


def split_path(path: str):
    if len(path.split("/")) > 0:
        file_name = path.split("/")[-1]
        file_path_arr = path.split("/")[:-1]
        file_path = "/".join(file_path_arr)
    else:
        file_name = path
        file_path = ""

    return {"file_name": file_name, "file_path": file_path}


def calculate_checksum(file_path: str):
    if ".zarr" not in file_path:
        with open(file_path, "rb") as f:
            file_hash = hashlib.md5()
            while chunk := f.read(8192):
                file_hash.update(chunk)
        return file_hash.hexdigest()
    else:
        return compute_zarr_checksum(yield_files_local(file_path)).md5


class DLUFile:
    def __init__(self, name: str, path: str, checksum: str, size: int, metadata: dict = {}):
        self.name = name
        self.path = path
        self.checksum = checksum
        self.size = size
        self.file_id = str(uuid.uuid4())
        self.metadata = metadata


class DirectoryInfo:
    def __init__(self, directory_path: str, calculate_checksums: bool = True):
        self.dir_contents = os.listdir(directory_path)
        self.subdir_count = 0
        self.file_count = 0
        self.file_details = []
        self.valid_for_dlu = False
        self.directory_path = directory_path
        self.calculate_checksums = calculate_checksums
        self.get_directory_information()
        self.check_if_valid_for_dlu()

    def get_directory_information(self):
        for item in self.dir_contents:
            full_path = os.path.join(self.directory_path, item)
            if os.path.isdir(full_path) and ".zarr" not in full_path:
                self.subdir_count += 1
            else:
                self.file_count += 1
                checksum = "0" if self.calculate_checksums else calculate_checksum(full_path)
                self.file_details.append(DLUFile(item, full_path, checksum, os.path.getsize(full_path)))

    def check_if_valid_for_dlu(self):
        directory_not_empty = len(self.dir_contents) != 0
        # Nothing but a subdir
        if self.subdir_count == 1 and self.file_count == 0:
            subdir_path = os.path.join(self.directory_path, self.dir_contents[0])
            subdir_info = DirectoryInfo(subdir_path, self.calculate_checksums)
            if subdir_info.subdir_count == 0 and subdir_info.valid_for_dlu:
                self.valid_for_dlu = True
            else:
                self.valid_for_dlu = False
        # Items and no subdirectories
        elif self.subdir_count == 0 and directory_not_empty:
            self.valid_for_dlu = True
        else:
            self.valid_for_dlu = False


class DLUFileHandler:

    def __init__(self):
        self.globus_data_directory = os.environ.get('globus_data_directory')
        self.dlu_data_directory = os.environ.get('dlu_data_directory')

    def copy_files(self, package_id: str, file_list: list[DLUFile], preserve_path: bool = False):
        files_copied = 0
        for file in file_list:
            source_package_directory = self.globus_data_directory + '/' + package_id
            if preserve_path:
                dest_package_directory = os.path.join(self.dlu_data_directory, DLU_PACKAGE_DIR_PREFIX + package_id,
                                                      file.path)
            else:
                dest_package_directory = os.path.join(self.dlu_data_directory, DLU_PACKAGE_DIR_PREFIX + package_id)
            if not os.path.exists(dest_package_directory):
                logger.info("Creating directory " + dest_package_directory)
                os.makedirs(dest_package_directory, exist_ok=True)
            source_file = os.path.join(source_package_directory, file.name)
            dest_file = os.path.join(dest_package_directory, file.name)
            logger.info("Copying file to " + dest_file)
            if not os.path.exists(dest_file):
                shutil.copy(source_file, dest_file)
                files_copied = files_copied + 1
            else:
                logger.warning(dest_file + " already exists. Skipping.")
        return files_copied

    def validate_package_directories(self, package_id: str):
        logger.info("Moving files for package " + package_id)
        source_package_directory = self.globus_data_directory + '/' + package_id
        source_directory_info = DirectoryInfo(source_package_directory, False)
        success = False

        # Make sure the directory is not empty and does not have more than one subdirectory.
        if source_directory_info.valid_for_dlu:
            # Set the source path to the subdirectory if it has only one and is valid.
            if source_directory_info.subdir_count == 1 and source_directory_info.file_count == 0:
                source_package_directory = os.path.join(source_package_directory,
                                                        source_directory_info.dir_contents[0])
                source_directory_info = DirectoryInfo(source_package_directory, False)
                logger.info(
                    "Found one subdirectory (" + source_package_directory + "). Setting it as the main data directory.")
            success = True
        else:
            success = False
            logger.error("Directory for package " + package_id + " failed validation.")
        return success
