import argparse
import logging
from services.dlu_mongo import DLUMongo
from lib.mongo_connection import MongoConnection
from services.data_management import DataManagement
from dotenv import load_dotenv
from services.dlu_filesystem import calculate_checksum, DLUFile
import os

logger = logging.getLogger("md5-updater")
logger.setLevel(logging.INFO)
load_dotenv()


class Main:
    def __init__(self):
        self.mongo_connection = MongoConnection().get_mongo_connection()
        self.dlu_mongo = DLUMongo(self.mongo_connection)
        self.data_management = DataManagement()
        self.data_lake_directory = os.environ["dlu_data_directory"]

    def report_mongo_records_missing_md5s(self):
        packages_missing_checksums = self.dlu_mongo.find_all_packages_missing_md5s()
        logger.info("Mongo records missing md5checksum")
        for package in packages_missing_checksums:
            for file in package["files"]:
                if "md5Checksum" not in file:
                   logger.error("file uuid: " + file["_id"] + " in package :" + package["_id"] + " missing md5")

    def report_mongo_records_incorrect_md5s(self):
        pass

    def report_dmd_records_missing_md5s(self):
        logger.info("DMD records missing md5checksum")
        files = self.data_management.find_files_missing_md5()
        logger.info(files)
        for file in files:
            logger.error("file uuid: " + file["dlu_file_id"] + " in package: " + file["dlu_package_id"] + " missing md5")


    def report_dmd_records_incorrect_md5s(self):
        pass

    def fill_mongo_missing_md5s(self):
        packages_missing_checksums = self.dlu_mongo.find_all_packages_missing_md5s()
        for package in packages_missing_checksums:
            # build path to file
            package_files = []
            for file in package["files"]:
                if "md5Checksum" not in file:
                    full_path = os.path.join(self.data_lake_directory, "package_" + package["_id"] + "/"
                                             + file["fileName"])
                    new_checksum = calculate_checksum(full_path)
                    dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="", checksum=new_checksum,
                                       file_id=file["_id"])
                    package_files.append(dlu_file)
                else:
                    dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="", checksum=file['md5Checksum'],
                                       file_id=file["_id"])
                    package_files.append(dlu_file)
                self.dlu_mongo.update_package_files(package["_id"], package_files)

    def fix_mongo_md5s(self):
        pass

    def fill_dmd_missing_md5s(self):
        pass

    def fix_dmd_md5s(self):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d",
                        "--dryrun",
                        required=False,
                        action='store_true',
                        help="Do not do updates, just report",
    )
    parser.add_argument("-m",
                        "--fill_missing",
                        required=False,
                        action='store_true',
                        help='Find and fill in missing md5s',
    )
    args = parser.parse_args()
    main = Main()
    if args.dryrun:
        main.report_mongo_records_missing_md5s()
        main.report_mongo_records_incorrect_md5s()
        main.report_dmd_records_missing_md5s()
        main.report_dmd_records_incorrect_md5s()
    elif args.fill_missing:
        main.fill_mongo_missing_md5s()
        main.fill_dmd_missing_md5s()
    else:
        main.fix_mongo_md5s()
        main.fix_dmd_md5s()