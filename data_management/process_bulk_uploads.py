import sys
from services.data_management import DataManagement
from services.dlu_filesystem import DLUFile
from services.dlu_mongo import PackageType
from model.dlu_package import DLUPackage
import argparse
import logging
import yaml
import os
import datetime
from bson.dbref import DBRef
from bson import ObjectId

logger = logging.getLogger("ProcessBulkUploads")
logger.setLevel(logging.INFO)

MANIFEST_FILE_NAME = "bulk-manifest-test.yaml"


class ProcessBulkUploads:
    def __init__(self, data_directory: str):
        try:
            self.data_management = DataManagement()
        except:
            logger.error("There was a problem loading the Data Management library.")
        try:
            self.submitter = os.environ["mongo_submitter_id"]
        except:
            self.submitter = os.environ.get("mongo_submitter_id")

        self.data_directory = data_directory

    def process_files(self, manifest_files_arr: list) -> list:
        dlu_files = []
        for file in manifest_files_arr:
            file_full_path = file["relative_file_path_and_name"]
            size = os.path.getsize(os.path.join(self.data_directory, file_full_path))
            file_path = file_full_path.split("/")[0]
            file_name = file_full_path.split("/")[1]
            dlu_file = DLUFile(file_name, file_path, file["file_metadata"]["md5_hash"], size)
            dlu_files.append(dlu_file)
        return dlu_files

    def process_bulk_uploads(self):
        manifest_file_path = os.path.join(self.data_directory, MANIFEST_FILE_NAME)
        if os.path.isfile(manifest_file_path):
            with open(manifest_file_path, "r") as stream:
                manifest_data = yaml.safe_load(stream)
                if manifest_data["package_type"] == "EM Images":
                    package_type = PackageType.ELECTRON_MICROSCOPY.value
                else:
                    package_type = PackageType.OTHER.value
                for experiment in manifest_data["experiments"]:
                    experiment = experiment["experiment"]
                    redcap_id = experiment["files"][0]["redcap_id"]
                    sample_id = experiment["files"][0]["spectrack_sample_id"]
                    logger.info(f"Trying to add package for {redcap_id}")
                    result = self.data_management.dlu_mongo.find_by_package_type_and_redcap_id(package_type, sample_id)
                    if result is None:
                        logger.info(f"Adding package for {redcap_id}")
                        package = DLUPackage()
                        package.dlu_package_type = package_type
                        # package.dlu_tis = experiment["redcap_data_access_group"]
                        package.dlu_tis = "Michigan/Broad/Princeton"
                        package.dlu_created = datetime.datetime.today()
                        package.dlu_submitter = DBRef(collection='users', id=ObjectId(self.submitter))
                        package.dlu_data_generators = manifest_data["data_generators"]
                        package.dlu_protocol = ""
                        package.dlu_description = manifest_data["dataset_description"]
                        package.dlu_tis_experiment_id = experiment["internal_experiment_id"]
                        package.dlu_subject_id = sample_id
                        package.dlu_lfu = True
                        package.redcap_id = redcap_id
                        package.known_specimen = sample_id
                        package.dlu_version = 4
                        package.dlu_dataset_information_version = 1
                        package_id = self.data_management.dlu_mongo.add_package(package.get_mongo_dict())
                        dlu_file_list = self.process_files(experiment["files"])
                        records_modified = self.data_management.dlu_mongo.update_package_files(package_id, dlu_file_list)
                        if records_modified == 1:
                            logger.info(f"{len(dlu_file_list)} files added to package {package_id}")
                        else:
                            logger.error(f"There was a problem adding files to package {package_id}")
                    else:
                        logger.info(f"A package for {redcap_id} already exists, skipping.")
            stream.close()
        else:
            logger.error("Manifest file doesn't exist.")
            sys.exit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data_directory",
        required=True,
        help="Location of the manifest file.",
    )
    args = parser.parse_args()
    process_bulk_uploads = ProcessBulkUploads(args.data_directory)
    process_bulk_uploads.process_bulk_uploads()
