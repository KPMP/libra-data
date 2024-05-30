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
from dateutil import tz
from bson.dbref import DBRef
from bson import ObjectId

logger = logging.getLogger("ProcessBulkUploads")
logger.setLevel(logging.INFO)

MANIFEST_FILE_NAMES = [
    "bulk-manifest.yaml",
    "bulk_manifest.yml"
    ]
SEGMENTATION_README = "README.md"


class ProcessBulkUploads:
    def __init__(self, data_directory: str, globus_only: bool = False, globus_root: str = None, preserve_path: bool = False):
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
        self.preserve_path = preserve_path
        self.globus_only = globus_only
        self.dlu_file_handler = DLUFileHandler()
        self.dlu_file_handler.globus_data_directory = data_directory
        self.dlu_file_handler.dlu_data_directory = self.dlu_data_directory
        # If the destination is Globus, set the destination data directory to the Globus root and don't use package prefix.
        if globus_only:
            self.dlu_file_handler.dlu_data_directory = globus_root
            self.dlu_file_handler.dlu_package_dir_prefix = ''

        self.dlu_state = DLUState()

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
        for manifest_name in MANIFEST_FILE_NAMES:
            manifest_file_path = os.path.join(self.data_directory, manifest_name)
            if os.path.isfile(manifest_file_path):
                break
            logger.error("Manifest file doesn't exist.")
            sys.exit()

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
                    redcap_results = self.data_management.get_redcap_id_by_spectrack_sample_id(sample_id)
                    if len(redcap_results) == 1:
                        redcap_id = redcap_results[0]["spectrack_redcap_record_id"]
                    else:
                        redcap_id = ""

                if not sample_id:
                    sample_id = redcap_id

                if sample_id and len(self.data_management.get_participant_by_redcap_id(redcap_id)) > 0:
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
                        package.dlu_created = datetime.datetime.now(tz.gettz('America/New_York'))
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
                        if self.globus_only:
                            package.globus_dlu_status = None
                        else:
                            package.globus_dlu_status = 'success'
                        package_id = self.data_management.dlu_mongo.add_package(package.get_mongo_dict())
                        self.dlu_state.set_package_state(package_id, PackageState.METADATA_RECEIVED)
                        self.data_management.insert_dlu_package(package.get_mysql_tuple())
                        if not self.globus_only:
                            logger.info("Copying files to DLU.")
                            records_modified = self.data_management.dlu_mongo.update_package_files(package_id, dlu_file_list)
                            self.data_management.insert_dlu_files(package.package_id, dlu_file_list)
                            if records_modified == 1:
                                logger.info(f"{len(dlu_file_list)} files added to package {package_id}")
                                files_copied = self.dlu_file_handler.copy_files(package_id, dlu_file_list, self.preserve_path, True)
                                if files_copied == len(dlu_file_list):
                                    self.dlu_state.set_package_state(package_id, PackageState.UPLOAD_SUCCEEDED)
                                    logger.info(f"{files_copied} files copied to DLU.")
                            else:
                                    logger.error(f"There was a problem adding files to package {package_id}")
                        else:
                            logger.info("Copying files to Globus.")
                            files_copied = self.dlu_file_handler.copy_files(package_id, dlu_file_list, self.preserve_path, True)
                            if files_copied == len(dlu_file_list):
                                logger.info(f"{files_copied} files copied to Globus.")

                    else:
                        package_id = result["_id"]
                        logger.info(f"A package for {redcap_id} already exists as package {package_id}, skipping.")

                else:
                    logger.info(f"No sample ID or Redcap ID {redcap_id} doesn't exist. Could this be a README? Skipping.")

        stream.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--data_directory",
        required=True,
        help="Location of the manifest file and data.",
    )
    parser.add_argument(
        '-g',
        '--globus_only',
        action='store_true',
        default=False,
        help='Only move files to Globus and do no process and put in DLU. Requires --globus_root argument if set.'
    )
    parser.add_argument(
        '-r',
        '--globus_root',
        required=False,
        default=None,
        help='The top-level Globus folder if globus_only is set.'
    )
    parser.add_argument(
        '-p',
        '--preserve_path',
        action='store_true',
        required=False,
        default=False,
        help='Preserve the file paths, i.e. do not flatten file structure.'
    )
    args = parser.parse_args()
    if args.globus_only and args.globus_root is None:
        parser.error("--globus_only requires --globus_root to be set.")
    process_bulk_uploads = ProcessBulkUploads(args.data_directory, args.globus_only, args.globus_root, args.preserve_path)
    process_bulk_uploads.process_bulk_uploads()
