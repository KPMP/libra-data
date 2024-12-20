import argparse
import logging
from services.dlu_mongo import DLUMongo
from lib.mongo_connection import MongoConnection
from services.dlu_management import DluManagement
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
        self.dlu_management = DluManagement()
        self.data_lake_directory = os.environ["checker_dlu_data_directory"]

    def fill_mongo_missing_md5s(self, report_only: bool = False):
        logger.info("Handling Mongo records missing md5checksum")
        packages_missing_checksums = self.dlu_mongo.find_all_packages_missing_md5s()
        for package in packages_missing_checksums:
            package_files = []
            for file in package["files"]:
                if "md5Checksum" not in file:
                    if report_only:
                        logger.error("file uuid: " + file["_id"] + " in package :" + package["_id"] + " missing md5")
                    else:
                        new_checksum = self.calculate_md5(file_name=file['fileName'], package_id=package["_id"])
                        if new_checksum is not None:
                            dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="", checksum=new_checksum,
                                               file_id=file["_id"])
                        else:
                            dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="", checksum="",
                                               file_id=file["_id"])
                        package_files.append(dlu_file)
                else:
                    dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="", checksum=file['md5Checksum'],
                                       file_id=file["_id"])
                    package_files.append(dlu_file)
                self.dlu_mongo.update_package_files(package["_id"], package_files)
        packages_missing_checksums.close()

    def fix_mongo_md5s(self, report_only: bool = False, fill_missing_only: bool = False):
        if fill_missing_only:
            self.fill_mongo_missing_md5s(report_only=report_only)
        else:
            logger.info("Handling Mongo records with incorrect md5checksums")
            all_packages = self.dlu_mongo.find_all_packages()
            for package in all_packages:
                package_files = []
                for file in package['files']:
                    checksum = self.calculate_md5(file_name=file['fileName'], package_id=package["_id"])
                    logger.info(checksum)
                    if report_only:
                        if "md5Checksum" not in file:
                            logger.error(
                                "file uuid: " + file["_id"] + " in package :" + package["_id"] + " missing md5")
                        elif file["md5Checksum"] != checksum and checksum is not None:
                            logger.error("file uuid: " + file["_id"] + " in package: " + package['_id'] +
                                         " incorrect md5")

                    if "md5Checksum" not in file or file["md5Checksum"] != checksum and checksum is not None:
                        dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="", checksum=checksum,
                                           file_id=file["_id"])
                        package_files.append(dlu_file)
                    else:
                        dlu_file = DLUFile(name=file["fileName"], size=file["size"], path="",
                                           checksum=file['md5Checksum'], file_id=file["_id"])
                        package_files.append(dlu_file)
                    self.dlu_mongo.update_package_files(package["_id"], package_files)
            all_packages.close()

    def fill_dmd_missing_md5s(self, report_only: bool = False):
        logger.info("Handling DMD records missing md5checksum")
        files = self.dlu_management.find_files_missing_md5()
        for file in files:
            if report_only:
                logger.error(
                    "file uuid: " + file["dlu_file_id"] + " in package: " + file["dlu_package_id"] + " missing md5")
            else:
                new_checksum = self.calculate_md5(file_name=file["dlu_fileName"], package_id=file["dlu_package_id"])
                if new_checksum is not None:
                    self.dlu_management.update_md5(file["dlu_file_id"], new_checksum, file["dlu_package_id"])

    def fix_dmd_md5s(self, report_only: bool = False, fill_missing_only: bool = False):
        if fill_missing_only:
            self.fill_dmd_missing_md5s(report_only=report_only)
        else:
            files = self.dlu_management.find_all_files()
            logger.info("Handling DMD records with incorrect md5checksums")
            for file in files:
                checksum = self.calculate_md5(file_name=file["dlu_fileName"], package_id=file["dlu_package_id"])
                logger.info(checksum)
                if report_only is True:
                    if file["dlu_md5checksum"] is None:
                        logger.error(
                            "file uuid: " + file["dlu_file_id"] + " in package :" + file["dlu_package_id"] +
                            " missing md5")
                    elif file["dlu_md5checksum"] != checksum and checksum is not None:
                        logger.error("file uuid: " + file["dlu_file_id"] + " in package: " + file["dlu_package_id"] +
                                     " incorrect md5")
                else:
                    if file["dlu_md5checksum"] is None or file["dlu_md5checksum"] != checksum and checksum is not None:
                        self.dlu_management.update_md5(file["dlu_file_id"], checksum, file["dlu_package_id"])

    def calculate_md5(self, file_name, package_id):
        full_path = os.path.join(self.data_lake_directory, "package_" + package_id + "/"
                                 + file_name)
        logger.info(full_path);
        if os.path.isfile(full_path):
            new_checksum = calculate_checksum(full_path)
            return new_checksum
        else:
            logger.error("file : " + full_path + " not found")
            return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d",
                        "--dryrun",
                        required=False,
                        action='store_true',
                        help="No updates, just reporting",
    )
    parser.add_argument("-m",
                        "--fill_missing",
                        required=False,
                        action='store_true',
                        help='Find and fill in missing md5s. Will not fix incorrect md5s',
    )
    parser.add_argument("-f",
                        "--full_fix",
                        default=True,
                        action='store_true',
                        help='DEFAULT: Will run with no option selected. Fills in missing md5s AND fixes incorrect md5s')
    args = parser.parse_args()
    main = Main()
    if args.dryrun:
        logger.info("Dry run will report only")
        main.fill_mongo_missing_md5s(report_only=True)
        main.fix_mongo_md5s(report_only=True)
        main.fill_dmd_missing_md5s(report_only=True)
        main.fix_dmd_md5s(report_only=True)
    elif args.fill_missing:
        logger.info("Fill missing will only fill in the files with no or null md5checksums")
        main.fix_mongo_md5s(report_only=False, fill_missing_only=True)
        main.fix_dmd_md5s(report_only=False, fill_missing_only=True)
    else:
        logger.info("Default behavior will fix ALL md5s (missing and incorrect)")
        main.fix_mongo_md5s(report_only=False, fill_missing_only=False)
        main.fix_dmd_md5s(report_only=False, fill_missing_only=False)
