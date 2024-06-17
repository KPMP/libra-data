import argparse
import logging
from services.dlu_mongo import DLUMongo
from lib.mongo_connection import MongoConnection
from dotenv import load_dotenv

logger = logging.getLogger("md5-updater")
logger.setLevel(logging.INFO)
load_dotenv()


class Main:
    def __init__(self):
        self.mongo_connection = MongoConnection().get_mongo_connection()
        self.dlu_mongo = DLUMongo(self.mongo_connection)

    def report_mongo_records_missing_md5s(self):
        packages_missing_checksums = self.dlu_mongo.find_all_packages_missing_md5s()
        logger.warning(packages_missing_checksums)

    def report_mongo_records_incorrect_md5s(self):
        pass

    def report_dmd_records_missing_md5s(self):
        pass

    def report_dmd_records_incorrect_md5s(self):
        pass

    def fix_mongo_md5s(self):
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
    args = parser.parse_args()
    main = Main()
    if args.dryrun:
        main.report_mongo_records_missing_md5s()
        main.report_mongo_records_incorrect_md5s()
        main.report_dmd_records_missing_md5s()
        main.report_dmd_records_incorrect_md5s()

    else:
        main.fix_mongo_md5s()
        main.fix_dmd_md5s()