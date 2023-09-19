import uuid


class DLUPackage:
    def __init__(self):
        self.package_id = str(uuid.uuid4())
        self.dlu_created = None
        self.dlu_submitter = None
        self.dlu_tis = None
        self.dlu_package_type = None
        self.dlu_subject_id = None
        self.dlu_error = None
        self.dlu_lfu = None
        self.dlu_description = None
        self.dlu_dataset_information_version = None
        self.dlu_version = None
        self.dlu_tis_experiment_id = None
        self.dlu_metadata_version = None
        self.dlu_protocol = None
        self.dlu_data_generators = None
        self.dlu_files = []
        self.submitter_name = None
        self.known_specimen = None
        self.redcap_id = None
        self.user_package_ready = None
        self.dvc_validation_complete = None
        self.package_validated = None
        self.ready_to_move_from_globus = None
        self.globus_dlu_status = None
        self.removed_from_globus = None
        self.promotion_status = None
        self.notes = None

    def get_mongo_dict(self):
        return {
            "_id": self.package_id,
            "largeFilesChecked": self.dlu_lfu,
            "description": self.dlu_description,
            "datasetInformationVersion": self.dlu_dataset_information_version,
            "tisInternalExperimentID": self.dlu_tis_experiment_id,
            "packageType": self.dlu_package_type,
            "version": self.dlu_version,
            "subjectId": self.dlu_subject_id,
            "tisName": self.dlu_tis,
            "protocol": self.dlu_protocol,
            "dataGenerators": self.dlu_data_generators,
            "files": self.dlu_files,
            "submitter": self.dlu_submitter,
            "createdAt": self.dlu_created
        }

    def get_mysql_tuple(self):
        return (
            self.package_id,
            self.dlu_created.strftime(
                "%Y-%m-%d"),
            self.submitter_name,
            self.dlu_tis,
            self.dlu_package_type,
            self.redcap_id,
            self.dlu_error,
            self.dlu_lfu,
            self.known_specimen,
            self.redcap_id,
            # Setting these to None for now until I talk to Rachel
            None,  # dlu_inventory["userPackageReady"],
            None,  # dlu_inventory["dvcValidationComplete"],
            None,  # dlu_inventory["packageValidated"],
            None,  # dlu_inventory["readyToMoveFromGlobus"],
            None,  # dlu_inventory["globusDluStatus"],
            None,  # dlu_inventory["removedFromGlobus"],
            None,  # dlu_inventory["promotionStatus"],
            None  # dlu_inventory["notes"],
        )
