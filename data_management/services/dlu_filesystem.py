import subprocess
import os
from pathlib import Path
import logging
import shutil
import filecmp
from hashlib import md5
import uuid
from zarr_checksum import compute_zarr_checksum
from zarr_checksum.generators import yield_files_local
from mmap import mmap, ACCESS_READ

logger = logging.getLogger("DLUFilesystem")
logger.setLevel(logging.INFO)

def calculate_checksum(file_path: str):

    if os.path.isdir(file_path):
        return "0"
    if ".zarr" not in file_path:
        with open(file_path) as f, mmap(f.fileno(), 0, access=ACCESS_READ) as f:
            return md5(f).hexdigest();
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

    # Returns path without top directory, i.e. package dir or participant dir (bulk uploads)
    def get_short_path(self):
        return "/".join(self.path.split("/")[1:])

    # Returns the filename without path prefix, if it has it.
    def get_short_filename(self):
        return self.name.split("/")[-1:][0]



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
                checksum = "0"
            else:
                self.file_count += 1
                checksum = "0" if not self.calculate_checksums else calculate_checksum(full_path)
            self.file_details.append(DLUFile(item, full_path, checksum, os.path.getsize(full_path)))

    def check_if_valid_for_dlu(self):
        self.valid_for_dlu = (len(self.dir_contents) != 0)


class DLUFileHandler:

    def __init__(self):
        self.globus_data_directory = '/globus'
        self.dlu_data_directory = '/data'
        self.dlu_package_dir_prefix = 'package_'

    def split_path(self, path: str, preserve_path: bool = False):
        if len(path.split("/")) > 0:
            if preserve_path:
                file_name = "/".join(path.replace(self.globus_data_directory, "").split("/")[1:])
            else:
                file_name = path.split("/")[-1]
            file_path_arr = path.split("/")[:-1]
            file_path = "/".join(file_path_arr)
        else:
            file_name = path
            file_path = ""

        return {"file_name": file_name, "file_path": file_path}
    
    def chown_dir(self, package_id: str):
        package_path = self.dlu_data_directory + "/" + package_id
        path = Path(package_path)
        if path.owner() != os.environ['dlu_user'] or path.group() != os.environ['dlu_group']:
            subprocess.call(["chown", "-R", os.environ['dlu_user'], os.environ['dlu_group'], package_path])
        

    def copy_files(self, package_id: str, file_list: list[DLUFile], preserve_path: bool = False, no_src_package: bool = False):
        files_copied = 0
        source_wd = os.getcwd()
        for file in file_list:
            source_package_directory = self.globus_data_directory + '/'
            # I.e. isn't a bulk upload that doesn't already have a package ID.
            if not no_src_package:
                source_package_directory = source_package_directory + package_id
            if file.path:
                source_package_directory = os.path.join(source_package_directory, file.path)
            if preserve_path:
                dest_package_directory = os.path.join(self.dlu_data_directory, self.dlu_package_dir_prefix + package_id,
                                                      file.get_short_path())
            else:
                dest_package_directory = os.path.join(self.dlu_data_directory, self.dlu_package_dir_prefix + package_id)

            # Is any of this code used? START >>
            subdirs = [os.path.join(source_package_directory, o)
            for o in os.listdir(source_package_directory)
              if os.path.isdir(os.path.join(source_package_directory, o))]
            dir = "".join(subdirs)
            if len(os.listdir(source_package_directory)) == 1 and os.path.isdir(source_package_directory) and os.path.isdir(dir):
                
                os.chdir(dir)
                allfiles = os.listdir(dir)

                for f in allfiles:
                    src_path = os.path.join(dir, f)
                    dst_path = os.path.join(dest_package_directory, f)
                    if not os.path.isdir(dest_package_directory):
                        os.mkdir(dest_package_directory)

                    if os.path.isfile(f):
                        logger.info("Copying file " + f + " to " + dst_path)
                        shutil.copy(src_path, dst_path)
                        files_copied += 1
                    else:
                        logger.info("Copying directory " + src_path)
                        files_copied += 1
                        shutil.copytree(src_path, dst_path)
                os.chdir(source_wd)
            # << END
            
            if not os.path.exists(dest_package_directory):
                logger.info("Creating directory " + dest_package_directory)
                os.makedirs(dest_package_directory, exist_ok=True)
            source_file = os.path.join(source_package_directory, file.get_short_filename())
            dest_file = os.path.join(dest_package_directory, file.get_short_filename())
            
            if not os.path.exists(dest_file) and not dest_file.find(dir) == -1:
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
