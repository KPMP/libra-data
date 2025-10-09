from lib.mongo_connection import MongoConnection
from services.dlu_filesystem import DLUFileHandler, DirectoryInfo, DLUFile
from services.dlu_package_inventory import DLUPackageInventory
from services.dlu_state import DLUState, PackageState
from services.dlu_management import DluManagement
from model.dlu_package import DLUPackage
from services.dlu_mongo import DLUMongo
from services.slide_management import SlideManagement

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
        self.slide_management = SlideManagement(self.dlu_management)
    
    def watch_for_packages(self):
        packages = self.db.get_dlu_package("yes")
        if len(packages) == 0:
            logger.info(
                "No records were found with status 'yes' "
            )
        else:
            self.update_packages_for_globus(packages)
            self.move_packages_to_DLU(packages)
    
    def watch_for_side_manifest_records(self):
        equal_num_rows = self.dlu_management.get_equal_num_rows()
        if equal_num_rows == 1:
            logger.info("No new records found in slide_manifest_import")
        else:
            self.update_slide_scan_curation()
    
    def update_slide_scan_curation(self):
        logger.info("Importing new row(s) into slide_scan_curation")
        self.slide_management.process_slide_manifest_imports()
            
    def update_packages_for_globus(self, packages):
        for index, package_result in enumerate(packages):
            logger.info("Setting file status to 'waiting' on package " + package_result['dlu_package_id'])
            self.db.set_dlu_package_waiting("yes", package_result['dlu_package_id'])

    def process_file_paths(self, file_list: list[DLUFile]) -> list:
        dlu_files = []
        for file in file_list:
            file.path = self.dlu_file_handler.split_path(file.path)['file_path']
            dlu_files.append(file)
        return dlu_files
    
    def pickup_waiting_packages(self):
        packages_in_waiting = self.db.get_waiting_packages()
        if len(packages_in_waiting) == 0:
            return  logger.info(
                "No records were found with status 'waiting'"
            )
        else:
            self.move_packages_to_DLU(packages_in_waiting)

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

            if package['dlu_packageType'] == 'Whole Slide Images' and package['globus_dlu_status'] != 'recalled':
                success = self.do_wsi_file_renames(globus_data_directory, package_id)
                if not success:
                    continue

            directory_info = DirectoryInfo(globus_data_directory)
            if not self.is_directory_valid(directory_info, package_id):
                continue

            if directory_info.file_count == 0 and directory_info.subdir_count == 1:
                contents = "".join(directory_info.dir_contents)
                top_level_subdir = package_id + "/" + contents
                file_list = self.dlu_file_handler.match_files(top_level_subdir)
            else:
              file_list = self.dlu_file_handler.match_files(package_id)

            self.dlu_file_handler.copy_files(package_id, self.process_file_paths(directory_info.file_details))
            self.dlu_file_handler.chown_dir(package_id, file_list, int(os.environ['dlu_user']))
            file_info = self.dlu_management.insert_dlu_files(package_id, file_list)
            self.dlu_management.update_dlu_package(package_id, { "globus_dlu_status": "success" })
            self.dlu_management.update_dlu_package(package_id, { "ready_to_move_from_globus": "done" })
            self.dlu_mongo.update_package_files(package_id, file_info)
            
            self.dlu_state.set_package_state(package_id, PackageState.UPLOAD_SUCCEEDED)
            self.dlu_state.clear_cache()

    def do_wsi_file_renames(self, globus_data_directory: str, package_id: str):
        error_msg = ""
        slide_scan_info = self.dlu_management.find_slide_scan_info_by_package_id(package_id)
        if slide_scan_info is None or len(slide_scan_info) == 0:
            error_msg = "Error: Package not found in slide_scan_v"

        missing_slides = self.dlu_management.is_package_missing_slides(package_id)
        if missing_slides is not None and len(missing_slides) > 0:
            error_msg = "Error: Package is missing slides"
        slides_in_error = self.dlu_management.is_slides_in_error(package_id)
        if slides_in_error is not None and len(slides_in_error) > 0:
            error_msg = "Error: Package has some slides in error"
        unapproved_files = self.dlu_management.find_not_approved_filenames(package_id)
        if unapproved_files is not None and len(unapproved_files) > 0:
            error_msg = "Error: Package has unapproved filenames"

        directory_info = DirectoryInfo(globus_data_directory)
        if not self.is_directory_valid(directory_info, package_id):
            return False

        if directory_info.file_count == 0 or directory_info.file_count != len(slide_scan_info):
            error_msg = "Error: Globus file count does not match expectation"

        file_list = self.dlu_file_handler.match_files(package_id)
        expected_slides = []
        for slide in slide_scan_info:
            expected_slides.append(slide['source_file_name'])
        for file in file_list:
            if file.name not in expected_slides:
                error_msg = "Error: Filenames in directory do not match slide_scan_curation info"
                continue

        if error_msg != "":
            self.dlu_management.update_dlu_package(package_id, {"globus_dlu_status": error_msg})
            return False

        return True

    def is_directory_valid(self, directory_info, package_id):
        if directory_info.file_count == 0 and directory_info.subdir_count == 0:
            error_msg = "Error: package " + package_id + " has no files or top level subdirectory"
            logger.info(error_msg + " Skipping.")
            self.dlu_management.update_dlu_package(package_id, {"globus_dlu_status": error_msg})
            return False


if __name__ == "__main__":
    dlu_watcher = DLUWatcher()
    dlu_watcher.pickup_waiting_packages()
    while True:
        dlu_watcher.watch_for_packages()
        dlu_watcher.watch_for_side_manifest_records()
        time.sleep(60)
