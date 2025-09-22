import sys
from services.dlu_management import DluManagement
from services.dlu_filesystem import DLUFile, DLUFileHandler, calculate_checksum
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
    def __init__(self, data_directory: str, globus_only: bool = False, globus_root: str = None, preserve_path: bool = False, bypass_dup_check: bool = False):
        try:
            self.dlu_management = DluManagement()
        except Exception as e:
            logger.exception("There was a problem loading the Data Management library.", e)
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
        self.bypass_dup_check = bypass_dup_check
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
        file_info = self.dlu_file_handler.split_path(file_path, self.preserve_path)
        return DLUFile(file_info["file_name"], file_info["file_path"], checksum, size, {})

    def process_files(self, manifest_files_arr: list) -> list:
        logger.info("processing files")
        dlu_files = []
        for file in manifest_files_arr:
            file_path = file["relative_file_path_and_name"]
            file_full_path = os.path.join(self.data_directory, file_path)
            logger.info(file_full_path)
            size = os.path.getsize(file_full_path)
            file_info = self.dlu_file_handler.split_path(file_path, self.preserve_path)
            if "file_metadata" in file and "md5_hash" in file["file_metadata"]:
                checksum = file["file_metadata"]["md5_hash"]
                del file["file_metadata"]["md5_hash"]
            else:
                checksum = calculate_checksum(file_full_path)
            if "file_metadata" in file:
                metadata = file["file_metadata"]
            else:
                metadata = {}
            dlu_file = DLUFile(file_info["file_name"], file_info["file_path"], checksum, size, metadata)
            dlu_files.append(dlu_file)
        return dlu_files

    def process_bulk_uploads(self):
        logger.info("in process bulk uploads")
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
            elif manifest_data["package_type"] == "Segmentation Masks & Pathomics Vectors":
                package_type = PackageType.SEGMENTATION
            elif manifest_data["package_type"] == "Multimodal Images":
                package_type = PackageType.MULTI_MODAL
            elif manifest_data["package_type"] == "Single-cell RNA-Seq":
                package_type = PackageType.SINGLE_CELL
            else:
                logger.info("package type is: ", manifest_data["package_type"])
                package_type = PackageType.OTHER
            if "tis" in manifest_data:
                tis = manifest_data["tis"]
            else:
                logger.error("Error: Missing TIS in manifest file.")
                sys.exit()
            for experiment in manifest_data["experiments"]:
                experiment = experiment["experiment"]
                redcap_id = experiment["files"][0]["redcap_id"]
                sample_id = experiment["files"][0]["spectrack_sample_id"]
                if redcap_id and redcap_id.startswith("S-"):
                    sample_id = redcap_id
                    redcap_results = self.dlu_management.get_redcapid_by_subjectid(sample_id)
                    if redcap_results is  not None and len(redcap_results) == 1:
                        redcap_id = redcap_results
                    else:
                        redcap_id = ""

                if not sample_id:
                    sample_id = redcap_id

                if (sample_id and len(self.dlu_management.get_participant_by_redcap_id(redcap_id)) > 0) or \
                        (self.globus_only and sample_id):
                    logger.info(f"Trying to add package for {redcap_id} / {sample_id}")
                    dlu_file_list = self.process_files(experiment["files"])
                    if package_type == PackageType.SEGMENTATION:
                        dlu_file_list.append(self.get_single_file(SEGMENTATION_README))
                        tis = "UFL"
                    logger.info("here")
                    result = self.dlu_management.dlu_mongo.find_by_package_type_and_redcap_id(package_type.value, sample_id)
                    logger.info("looked up package")
                    if result is None or self.bypass_dup_check:
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
                        package.known_specimen = ""
                        package.dlu_version = 4
                        package.dlu_dataset_information_version = 1
                        package.dlu_error = 0
                        package.dlu_upload_type = 'KPMP Biopsy';
                        if self.globus_only:
                            package.globus_dlu_status = None
                        else:
                            package.globus_dlu_status = 'success'
                        package_id = self.dlu_management.dlu_mongo.add_package(package.get_mongo_dict())
                        self.dlu_state.set_package_state(package_id, PackageState.METADATA_RECEIVED)
                        self.dlu_management.insert_dlu_package(package.get_dmd_dpi_tuple(), package.get_dmd_tuple())
                        if not self.globus_only:
                            logger.info("Copying files to DLU.")
                            records_modified = self.dlu_management.dlu_mongo.update_package_files(package_id, dlu_file_list)
                            self.dlu_management.insert_dlu_files(package.package_id, dlu_file_list)
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
                    logger.info(f"No sample ID {sample_id} or Redcap ID {redcap_id} doesn't exist. Could this be a README? Skipping.")

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
    parser.add_argument(
        '-b',
        '--bypass_dup_check',
        action='store_true',
        required=False,
        default=False,
        help='Bypass duplicate package check. Will create new packages when package exists for package type/redcap_id combo.'
    )
    args = parser.parse_args()
    if args.globus_only and args.globus_root is None:
        parser.error("--globus_only requires --globus_root to be set.")
    process_bulk_uploads = ProcessBulkUploads(args.data_directory, args.globus_only, args.globus_root, args.preserve_path, args.bypass_dup_check)
    process_bulk_uploads.process_bulk_uploads()
