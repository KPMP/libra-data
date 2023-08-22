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

DLU_PACKAGE_DIR_PREFIX = '/package_'
GLOBUS_DATA_DIRECTORY = '/globus'
DLU_DATA_DIRECTORY = '/data'

def create_dest_directory(dest_path: str):
    return_value = False
    if not os.path.exists(dest_path):
        logger.info("Destination directory " + dest_path + " does not exist. Creating.")
        os.mkdir(dest_path)
        return_value = True
    else:
        dest_dir_info = DirectoryInfo(dest_path)
        if dest_dir_info.file_count > 1:
            logger.error("Potential data files in destination directory.")
        elif dest_dir_info.file_count == 1:
            if os.path.exists(os.path.join(dest_path, "metadata.json")):
                logger.info("Deleting metadata.json.")
                os.remove(os.path.join(dest_path, "metadata.json"))
                return_value = True
            else:
                logger.error("Unknown file in target directory: " + dest_path)
        else:
            return_value = True
    return return_value


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
    def __init__(self, name: str, path: str, checksum: str, size: int):
        self.name = name
        self.path = path
        self.checksum = checksum
        self.size = size
        self.file_id = str(uuid.uuid4())


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


def copy_from_src_to_dest(source_path: str, dest_path: str):
    logger.info("Copying files from " + source_path + " to " + dest_path)
    shutil.copytree(source_path, dest_path, dirs_exist_ok=True)


class DLUFileHandler:

    def __init__(self):
        self.globus_data_directory = GLOBUS_DATA_DIRECTORY
        self.dlu_data_directory = DLU_DATA_DIRECTORY

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
