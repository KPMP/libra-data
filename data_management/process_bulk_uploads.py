import sys
from services.data_management import DataManagement
from services.dlu_filesystem import DLUFile, DLUFileHandler, calculate_checksum, split_path
from services.dlu_state import PackageState, DLUState
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

MANIFEST_FILE_NAME = "bulk-manifest-segmentation-test.yaml"
SEGMENTATION_README = "README.md"


class ProcessBulkUploads:
    def __init__(self, data_directory: str, move: bool):
        try:
            self.data_management = DataManagement()
        except:
            logger.error("There was a problem loading the Data Management library.")
        try:
            self.submitter = os.environ["mongo_submitter_id"]
            self.submitter_name = os.environ["submitter_name"]
            self.dlu_data_directory = os.environ["dlu_data_directory"]
        except:
            self.submitter = os.environ.get("mongo_submitter_id")
            self.submitter_name = os.environ.get("submitter_name")
            self.dlu_data_directory = os.environ.get("dlu_data_directory")

        self.data_directory = data_directory
        self.dlu_file_handler = DLUFileHandler()
        self.dlu_file_handler.globus_data_directory = data_directory
        self.dlu_file_handler.dlu_data_directory = self.dlu_data_directory
        self.dlu_state = DLUState()
        self.move = move

    def get_single_file(self, file_path: str) -> DLUFile:
        full_path = os.path.join(self.data_directory, file_path)
        size = os.path.getsize(full_path)
        checksum = calculate_checksum(full_path)
        file_info = split_path(file_path)
        return DLUFile(file_info["file_name"], file_info["file_path"], checksum, size, {})

    def process_files(self, manifest_files_arr: list) -> list:
        dlu_files = []
        for file in manifest_files_arr:
            file_path = file["relative_file_path_and_name"]
            file_full_path = os.path.join(self.data_directory, file_path)
            size = os.path.getsize(file_full_path)
            file_info = split_path(file_path)
            if file["file_metadata"] and "md5_hash" in file["file_metadata"]:
                checksum = file["file_metadata"]["md5_hash"]
                del file["file_metadata"]["md5_hash"]
            else:
                checksum = calculate_checksum(file_full_path)
            if file["file_metadata"]:
                metadata = file["file_metadata"]
            else:
                metadata = {}
            dlu_file = DLUFile(file_info["file_name"], file_info["file_path"], checksum, size, metadata)
            dlu_files.append(dlu_file)
        return dlu_files

    def process_bulk_uploads(self):
        manifest_file_path = os.path.join(self.data_directory, MANIFEST_FILE_NAME)
        if os.path.isfile(manifest_file_path):
            with open(manifest_file_path, "r") as stream:
                manifest_data = yaml.safe_load(stream)
                if manifest_data["package_type"] == "EM Images":
                    package_type = PackageType.ELECTRON_MICROSCOPY
                elif manifest_data["package_type"] == "Segmentation Masks":
                    package_type = PackageType.SEGMENTATION
                else:
                    package_type = PackageType.OTHER
                for experiment in manifest_data["experiments"]:
                    experiment = experiment["experiment"]
                    redcap_id = experiment["files"][0]["redcap_id"]
                    sample_id = experiment["files"][0]["spectrack_sample_id"]
                    if redcap_id and redcap_id.startswith("S-"):
                        sample_id = redcap_id
                        redcap_id = self.data_management.get_redcap_id_by_spectrack_sample_id(sample_id)

                    if not sample_id:
                        sample_id = redcap_id

                    if sample_id and len(self.data_management.get_participant_by_redcap_id(redcap_id)) > 0:
                        files_copied = 0
                        if "recruitment_site" in experiment:
                            tis = experiment["recruitment_site"]
                        else:
                            tis = ""
                        logger.info(f"Trying to add package for {redcap_id}")
                        dlu_file_list = self.process_files(experiment["files"])
                        if package_type == PackageType.SEGMENTATION:
                            dlu_file_list.append(self.get_single_file(SEGMENTATION_README))
                            tis = "UFL"
                        result = self.data_management.dlu_mongo.find_by_package_type_and_redcap_id(package_type.value, sample_id)
                        if result is None:
                            logger.info(f"Adding package for {redcap_id}")
                            package = DLUPackage()
                            package.dlu_package_type = package_type.value
                            package.dlu_tis = tis
                            package.dlu_created = datetime.datetime.today()
                            package.dlu_submitter = DBRef(collection='users', id=ObjectId(self.submitter))
                            package.submitter_name = self.submitter_name
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
                            self.data_management.insert_dlu_package(package.get_mysql_tuple())
                            records_modified = self.data_management.dlu_mongo.update_package_files(package_id, dlu_file_list)
                            self.data_management.insert_dlu_files(package.package_id, dlu_file_list)
                            self.dlu_state.set_package_state(package_id, PackageState.METADATA_RECEIVED)
                            if records_modified == 1:
                                logger.info(f"{len(dlu_file_list)} files added to package {package_id}")
                            else:
                                logger.error(f"There was a problem adding files to package {package_id}")
                        else:
                            package_id = result["_id"]
                            logger.info(f"A package for {redcap_id} already exists as package {package_id}, skipping.")
                        if self.move:
                            files_copied = self.dlu_file_handler.copy_files(package_id, dlu_file_list, False)
                            if files_copied == len(dlu_file_list):
                                self.dlu_state.set_package_state(package_id, PackageState.UPLOAD_SUCCEEDED)
                        logger.info(f"{files_copied} files copied to DLU.")
                    else:
                        logger.info(f"No sample ID or Redcap ID {redcap_id} doesn't exist. Could this be a README? Skipping.")

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
        help="Location of the manifest file and data.",
    )
    parser.add_argument(
        '-m',
        '--move',
        action='store_true',
        help='Move files to DLU.'
            )
    args = parser.parse_args()
    process_bulk_uploads = ProcessBulkUploads(args.data_directory, args.move)
    process_bulk_uploads.process_bulk_uploads()
