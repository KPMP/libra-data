from lib.mongo_connection import MongoConnection
from services.dlu_filesystem import DLUFileHandler, DirectoryInfo, DLUFile
from services.dlu_package_inventory import DLUPackageInventory
from services.dlu_state import DLUState, PackageState
from services.dlu_management import DluManagement
from model.dlu_package import DLUPackage
from services.dlu_mongo import DLUMongo

from dotenv import load_dotenv
import logging
import time
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
        self.dlu_management = DluManagement()
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
            file.path = self.dlu_file_handler.split_path(file.path)['file_path']
            dlu_files.append(file)
        return dlu_files
    
    def pickup_waiting_files(self):
        files_in_waiting = self.db.get_waiting_files()
        if len(files_in_waiting) == 0:
            return  logger.info(
                "No records were found with status 'waiting'"
            )
        else:
            self.move_packages_to_DLU(files_in_waiting)

    def move_packages_to_DLU(self, packages):
        file_list = None
        for _, package in enumerate(packages):
            package_id = package['dlu_package_id']
            logger.info("Moving package " + package_id)

            self.dlu_management.update_dlu_package(package_id, { "globus_dlu_status": "processing" })
            globus_data_directory = '/globus/' + package_id
            if not os.path.isdir(globus_data_directory):
                error_msg = "Error: package " + package_id + " not found in directory " + globus_data_directory + "."
                logger.info(error_msg + " Skipping.")
                self.dlu_management.update_dlu_package(package_id, { "globus_dlu_status": error_msg })
                continue

            directory_info = DirectoryInfo(globus_data_directory)

            if directory_info.file_count == 0 and directory_info.subdir_count == 0:
                error_msg = "Error: package " + package_id + " has no files or top level subdirectory"
                logger.info(error_msg + " Skipping.")
                self.dlu_management.update_dlu_package(package_id, { "globus_dlu_status": error_msg })
                continue
            
            if directory_info.file_count == 0 and directory_info.subdir_count == 1:
                contents = "".join(directory_info.dir_contents)
                top_level_subdir = package_id + "/" + contents
                file_list = self.dlu_file_handler.match_files(top_level_subdir)
            else:
              file_list = self.dlu_file_handler.match_files(package_id)

            self.dlu_file_handler.copy_files(package_id, self.process_file_paths(directory_info.file_details))
            self.dlu_file_handler.chown_dir(package_id, file_list)
            file_info = self.dlu_management.insert_dlu_files(package_id, file_list)
            self.dlu_management.update_dlu_package(package_id, { "globus_dlu_status": "success" })
            self.dlu_management.update_dlu_package(package_id, { "ready_to_move_from_globus": "done" })
            self.dlu_mongo.update_package_files(package_id, file_info)
            self.dlu_state.set_package_state(package_id, PackageState.UPLOAD_SUCCEEDED)
            self.dlu_state.clear_cache()



if __name__ == "__main__":
    dlu_watcher = DLUWatcher()
    dlu_watcher.pickup_waiting_files()
    while True:    
        dlu_watcher.watch_for_files()
        time.sleep(60) 