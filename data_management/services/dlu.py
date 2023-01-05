from datetime import datetime
from lib.mysql_connection import MYSQLConnection
from lib.mongo_connection import MongoConnection
import os
import logging
import shutil
import filecmp
import hashlib
import uuid
import requests

logger = logging.getLogger("ProcessLargeFileUpload")
logger.setLevel(logging.INFO)

DLU_PACKAGE_DIR_PREFIX = '/package_'
GLOBUS_DATA_DIRECTORY = '/globus'
DLU_DATA_DIRECTORY = '/data'

def dlu_package_dict_to_tuple(dlu_inventory: dict):
    # Java timestamp is in milliseconds
    dt_string = datetime.fromtimestamp(dlu_inventory["dluCreated"] / 1000.0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return (
        dlu_inventory["dluPackageId"],
        dt_string,
        dlu_inventory["dluSubmitter"],
        dlu_inventory["dluTis"],
        dlu_inventory["dluPackageType"],
        dlu_inventory["dluSubjectId"],
        dlu_inventory["dluError"],
        dlu_inventory["dluLfu"],
        dlu_inventory["knownSpecimen"],
        dlu_inventory["redcapId"],
        dlu_inventory["userPackageReady"],
        dlu_inventory["dvcValidationComplete"],
        dlu_inventory["packageValidated"],
        dlu_inventory["readyToPromoteDlu"],
        dlu_inventory["promotionDluSucceeded"],
        dlu_inventory["removedFromGlobus"],
        dlu_inventory["promotionStatus"],
        dlu_inventory["notes"],
    )


def dlu_file_dict_to_tuple(dlu_file: dict):
    return (
        dlu_file["dluFileName"],
        dlu_file["dluPackageId"],
        dlu_file["dluFileId"],
        dlu_file["dluFileSize"],
        dlu_file["dluMd5Checksum"],
    )


def create_dest_directory(dest_path: str):
    if not os.path.exists(dest_path):
        logger.info("Destination directory " + dest_path + " does not exist. Creating.")
        os.mkdir(dest_path)
    else:
        logger.info("Destination directory exists. Deleting metadata.json if it exists.")
        if os.path.exists(os.path.join(dest_path, "metadata.json")):
            os.remove(os.path.join(dest_path, "metadata.json"))


def calculate_checksum(file_path: str):
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)
    return file_hash.hexdigest()


class DirectoryInfo:
    def __init__(self, directory_path: str):
        self.dir_contents = os.listdir(directory_path)
        self.dir_count = 0
        self.file_count = 0
        self.file_details = []
        self.valid_for_dlu = False
        self.directory_path = directory_path
        self.get_directory_information()
        self.check_if_valid_for_dlu()

    def get_directory_information(self):
        for item in self.dir_contents:
            full_path = os.path.join(self.directory_path, item)
            if os.path.isdir(full_path):
                self.dir_count += 1
            else:
                self.file_count += 1
                self.file_details.append({
                    "name": item,
                    "path": full_path,
                    "checksum": calculate_checksum(full_path),
                    "size": os.path.getsize(full_path)
                })

    def check_if_valid_for_dlu(self):
        directory_not_empty = len(self.dir_contents) != 0
        # Nothing but a subdir
        if self.dir_count == 1 and self.file_count == 1:
            self.valid_for_dlu = True
        # Items and no subdirectories
        elif self.dir_count == 0 and directory_not_empty:
            self.valid_for_dlu = True
        else:
            self.valid_for_dlu = False


def copy_from_src_to_dest(source_path: str, dest_path: str):
    logger.info("Copying files from " + source_path + " to " + dest_path)
    shutil.copytree(source_path, dest_path, dirs_exist_ok=True)


class DLUFileHandler:

    def __init__(self, mysql_db: MYSQLConnection, mongo_connection: MongoConnection):
        self.dmd_database = mysql_db.get_db_connection()
        self.package_collection = mongo_connection.packages
        self.state_url = "http://state-spring:3060/v1/state/host/upload_kpmp_org"
        self.cache_clear_url = "http://localhost:3030/v1/clearCache"
        self.globus_data_directory = GLOBUS_DATA_DIRECTORY
        self.dlu_data_directory = DLU_DATA_DIRECTORY

    def move_files_from_globus(self, package_id: str):
        logger.info("Moving files for package " + package_id)
        source_package_directory = self.globus_data_directory + '/' + package_id
        dest_package_directory = self.dlu_data_directory + DLU_PACKAGE_DIR_PREFIX + package_id
        source_directory_info = DirectoryInfo(source_package_directory)

        # Make sure the directory is not empty and does not have more than one subdirectory.
        if source_directory_info.valid_for_dlu:
            # Set the source path to the subdirectory if it has only one and is valid.
            if source_directory_info.dir_count == 1 and source_directory_info.file_count == 0:
                source_package_directory = os.path.join(source_package_directory,
                                                        source_directory_info.dir_contents[0])
                source_directory_info = DirectoryInfo(source_package_directory)
                logger.info(
                    "Found one subdirectory (" + source_package_directory + "). Setting it as the main data directory.")
                if not source_directory_info.valid_for_dlu \
                        or source_directory_info.dir_count > 0:
                    logger.error("The subdirectory is not valid or there are too many nested subdirectories.")
                    os.sys.exit()

            create_dest_directory(dest_package_directory)
            copy_from_src_to_dest(source_package_directory, dest_package_directory)
            file_list = filecmp.dircmp(source_package_directory, dest_package_directory)
            if file_list == 0:
                logger.info("Package " + package_id + " moved successfully.")
                return True
            else:
                logger.error("The source and destination directory files do not match.")
                return False
        else:
            logger.error("Directory for package " + package_id + " failed validation.")
            return False

    def update_mongo(self, package_id: str):
        directory_info = DirectoryInfo(self.dlu_data_directory + DLU_PACKAGE_DIR_PREFIX + package_id)
        files = []
        for file in directory_info.file_details:
            file_id = uuid.uuid4()
            files.append({
                "dluFileName": file["name"],
                "dluPackageId": package_id,
                "dluFileId": file_id,
                "dluFileSize": file["size"],
                "dluMd5Checksum": file["checksum"],
            })
        self.package_collection.update_one({"_id": package_id}, {"$set": {"files": files, "regenerateZip": True}})

    def update_state(self, package_id: str):
        data = {
            "packageId": package_id,
            "state": "UPLOAD_SUCCEEDED",
            "largeUploadChecked": True
        }
        try:
            requests.post(self.state_url, data)
        except requests.exceptions.RequestException as e:
            logger.error("There was an error updating the state: " + e)
        requests.get(self.cache_clear_url)
