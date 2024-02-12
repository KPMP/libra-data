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
        return len(self.dir_contents) != 0


class DLUFileHandler:

    def __init__(self):
        self.globus_data_directory = '/globus'
        self.dlu_data_directory = '/data'

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
            if not os.path.exists(dest_file):
                if os.path.isdir(source_file):
                    logger.info("Copying directory to " + dest_file)
                    shutil.copytree(source_file, dest_file)
                elif os.path.isfile(source_file):
                    logger.info("Copying file to " + dest_file)
                    shutil.copy(source_file, dest_file)
                files_copied = files_copied + 1
            else:
                logger.warning(dest_file + " already exists. Skipping.")
        return files_copied

    def validate_package_directories(self, package_id: str):
        source_package_directory = self.globus_data_directory + '/' + package_id
        source_directory_info = DirectoryInfo(source_package_directory, False)
        success = True

        # Make sure the directory is not empty
        if not source_directory_info.valid_for_dlu:
            success = False
            logger.error("Directory for package " + package_id + " failed validation.")
        return success

    def process_globus_directory(self, directoryListing, globusDirectories: list[DirectoryInfo], packageId, initialDir):
        for dir in globusDirectories:
            prefix = ""
            if not initialDir == "":
                prefix = initialDir + "/"
            currentDir = prefix + os.path.basename(dir.directory_path)

            globusFiles = []
            globusDirectories = []
            for item in dir.file_details:
                if os.path.isdir(item.path):
                    globusDirectories.append(DirectoryInfo(item.path))
                else:
                    globusFiles.append(item)
            directoryListing[currentDir] = globusFiles
            if len(globusDirectories) > 0: 
                self.process_globus_directory(directoryListing, globusDirectories, packageId, currentDir)
        return directoryListing
    
    def match_files(self, packageId) -> list[DLUFile]:
        topLevelDir = DirectoryInfo(self.globus_data_directory + '/' + packageId)
        globusFiles = []
        globusDirectories = []
        for obj in topLevelDir.file_details:
            if os.path.isdir(obj.path):
                directory = DirectoryInfo(obj.path)
                globusDirectories.append(directory)
            else:
                globusFiles.append(obj)
        filesInGlobusDirectories = {}
        filesInGlobusDirectories[""] = globusFiles
        currentDir = ""
        filesInGlobusDirectories = self.process_globus_directory(filesInGlobusDirectories, globusDirectories, packageId, currentDir)
        return self.get_globus_file_paths(filesInGlobusDirectories)
    
    def get_globus_file_paths(self, filesInGlobusDirectories: dict[str, list[DLUFile]]) -> list[DLUFile]:
        fileList = []
        for dir, files in filesInGlobusDirectories.items():
            for file in files:
                prefix = dir + "/" if dir else ""
                file.name = prefix + file.name
                fileList.append(file)
        return fileList
