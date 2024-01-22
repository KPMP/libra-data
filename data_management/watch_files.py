from lib.mongo_connection import MongoConnection
from bson.dbref import DBRef
from bson import ObjectId

from services.dlu_filesystem import DLUFileHandler, DirectoryInfo, DLUFile, split_path
from services.dlu_package_inventory import DLUPackageInventory
from services.dlu_state import DLUState, PackageState
from services.data_management import DataManagement
from model.dlu_package import DLUPackage
from services.dlu_mongo import DLUMongo

from dotenv import load_dotenv
import logging
import time
import datetime
import os

logger = logging.getLogger("services-dlu_package_watcher")
logger.setLevel(logging.INFO)
load_dotenv()
  
class DLUWatcher:   
    def __init__ (self, db: DLUPackageInventory = None):
        if db:
            self.db = db
        else:
            self.db = DLUPackageInventory()
        self.mongo_connection = MongoConnection().get_mongo_connection()
        self.dlu_mongo = DLUMongo(self.mongo_connection)
        self.data_management = DataManagement()
        self.dlu_file_handler = DLUFileHandler()
        self.dluPackage = DLUPackage()
        self.dlu_state = DLUState()
    
    def watch_for_files(self):
        files = self.db.get_dlu_file("yes")
        if len(files) == 0:
            logger.info(
                "No records were found with status 'yes' "
            )
        else:
            self.update_files_for_globus(files)
            self.move_packages_to_DLU(files)
            
    def update_files_for_globus(self, files):
        for index, file_result in enumerate(files):
            logger.info("Setting file status to 'waiting' on package " + file_result['dlu_package_id'])
            self.db.set_dlu_file_waiting("yes", file_result['dlu_package_id'])

    def process_file_paths(self, file_list: list[DLUFile]) -> list:
        dlu_files = []
        for file in file_list:
            file.path = split_path(file.path)['file_path']
            dlu_files.append(file)
        return dlu_files

    def move_packages_to_DLU(self, packages):
        processing_count = self.db.get_processing_packages()
        if len(processing_count) < 2:
            for _, package in enumerate(packages):
                package_id = package['dlu_package_id']
                logger.info("Moving package " + package_id)

                self.data_management.update_dlu_package(package_id, { "globus_dlu_status": "processing" })
                globus_data_directory = os.environ.get('globus_data_directory') + '/' + package_id
                if not os.path.isdir(globus_data_directory):
                    error_msg = "Error: package " + package_id + " not found in directory " + os.environ.get('globus_data_directory') + "."
                    logger.info(error_msg + " Skipping.")
                    self.data_management.update_dlu_package(package_id, { "globus_dlu_status": error_msg })
                    continue
    
                directory_info = DirectoryInfo(globus_data_directory)
                directory_info.get_directory_information()

                if directory_info.file_count == 0:
                    error_msg = "Error: package " + package_id + " has no files."
                    logger.info(error_msg + " Skipping.")
                    self.data_management.update_dlu_package(package_id, { "globus_dlu_status": error_msg })
                    continue

                self.dlu_file_handler.copy_files(package_id, self.process_file_paths(directory_info.file_details))
                self.data_management.update_dlu_package(package_id, { "globus_dlu_status": "success" })
                self.dlu_state.set_package_state(package_id, PackageState.UPLOAD_SUCCEEDED)




if __name__ == "__main__":
    while True:
        dlu_watcher = DLUWatcher()
        dlu_watcher.watch_for_files()
        time.sleep(10*60)