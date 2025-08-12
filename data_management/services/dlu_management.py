from datetime import datetime

from lib.mysql_connection import MYSQLConnection
from lib.mongo_connection import MongoConnection
import logging
from services.dlu_filesystem import DLUFileHandler
from services.dlu_mongo import DLUMongo
from services.dlu_state import DLUState
from services.dlu_filesystem import DLUFile
from typing import List
import json

logger = logging.getLogger("services-dluManagement")
logger.setLevel(logging.INFO)


def get_update_query_info(fields_vals: dict):
    query_string = ""
    for field in fields_vals:
        query_string = query_string + field + " = %s, "
    query_obj = {"set_clause": query_string.rstrip(", "), "values": tuple(fields_vals.values())}
    return query_obj


class DluManagement:
    def __init__(self):
        self.db = MYSQLConnection()
        self.db.get_db_connection()
        self.mongo_connection = MongoConnection().get_mongo_connection()
        self.dlu_mongo = DLUMongo(self.mongo_connection)
        self.dlu_file_handler = DLUFileHandler()
        self.dlu_state = DLUState()

    def reconnect(self):
        self.db = MYSQLConnection()
        self.database = self.db.get_db_connection()

    def get_data_management_tables(self):
        data = self.db.get_data("SHOW TABLES;")
        logger.info("data:", data)

    def get_redcap_participant_count(self, redcap_id):
        return self.db.get_data(
            "SELECT count(redcap_id) as p_count FROM redcap_participant WHERE redcap_id = %s",
            (redcap_id,),
        )[0]["p_count"]

    def get_redcap_participant(self, redcap_id):
        return self.db.get_data(
            "SELECT * FROM redcap_participant WHERE redcap_id = %s", (redcap_id,)
        )[0]

    def insert_redcap_participant(self, redcap_participant):
        if self.get_redcap_participant_count(redcap_participant["redcap_id"]) == 0:
            logger.info(
                f"inserting redcap participant with id: {redcap_participant['redcap_id']}"
            )
            self.db.insert_data(
                "INSERT INTO redcap_participant ( "
                + "redcap_id, redcap_np_gender, redcap_age_binned, redcap_enrollment_category, "
                + "redcap_protocol, redcap_sample_type, redcap_tissue_source, redcap_clinical_data,"
                  "redcap_exp_aki_kdigo, redcap_exp_race, redcap_exp_alb_cat_most_recent,"
                  "redcap_mh_ht_yn, redcap_mh_diabetes_yn, redcap_exp_has_med_raas, "
                  "redcap_exp_a1c_cat_most_recent, redcap_exp_pro_cat_most_recent, "
                  "redcap_exp_diabetes_duration, redcap_exp_ht_duration, redcap_exp_egfr_bl_cat, redcap_adj_primary_category) "
                + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (list(redcap_participant.values())),
            )
            return 1
        else:
            logger.warning(
                f"redcap participant with id: {redcap_participant['redcap_id']} already exists, skipping insert"
            )
            return 0

    def get_participant_by_redcap_id(self, redcap_id: str):
        return self.db.get_data(
            "SELECT * FROM data_management.redcap_participant WHERE redcap_id = %s",(redcap_id,),
        )


    def insert_dlu_package(self, dpi_values: tuple, dmd_values: tuple):
        logger.info(f"inserting DLU package with id: {dpi_values[0]}")
        query1 = ("INSERT INTO dlu_package_inventory (dlu_package_id, dlu_created, dlu_submitter, dlu_tis, "
                 + "dlu_packageType, dlu_subject_id, dlu_error, dlu_lfu, dlu_upload_type, globus_dlu_status) "
                 + "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        self.db.insert_data(query1, dpi_values)
        dmd_values = ((dmd_values[0],) + dmd_values)
        query2 = ("INSERT INTO dmd_data_manager (id, dlu_package_id, redcap_id, known_specimen, "
                 + "user_package_ready, package_validated, ready_to_move_from_globus, "
                 + "removed_from_globus, ar_promotion_status, sv_promotion_status, notes) "
                 + "VALUES((SELECT id FROM dlu_package_inventory dpi WHERE dpi.dlu_package_id = %s), " 
                 + "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        self.db.insert_data(query2, (dmd_values))
        return (query1 % dpi_values) + ";\n" + (query2 % dmd_values)

    def update_dlu_package(self, package_id: str, fields_values: dict):
        if fields_values:
            logger.info(f"updating DLU package {package_id} with " + str(fields_values))
            query_info = get_update_query_info(fields_values)
            values = query_info["values"][0:] + (package_id,)
            query = "UPDATE data_manager_data_v SET " + query_info["set_clause"] + " WHERE dlu_package_id = %s"
            self.db.insert_data(query, values)

    def insert_dlu_file(self, values):
        query = "INSERT INTO dlu_file (dlu_fileName, dlu_package_id, dlu_file_id, dlu_filesize, dlu_md5checksum, dlu_modified_at, dlu_metadata) VALUES(%s, %s, %s, %s, %s, %s, %s)"
        self.db.insert_data(query, values)
        return query % values

    def insert_dlu_files(self, package_id: str, file_list: List[DLUFile]) -> dict:
        logger.info(f"Inserting files for package {package_id}")
        existing_files = self.get_files_by_package_id(package_id)
        unmodified_files = []
        if existing_files is not None and len(existing_files) > 0:
            logger.info(f"Deleting existing files for package {package_id}")
            self.delete_files_by_package_id(package_id)
        for file in file_list:
            for existing_file in existing_files:
                if existing_file["dlu_fileName"] == file.name:
                    file.modified_at = existing_file["dlu_modified_at"]
                    if file.checksum != existing_file["dlu_md5checksum"]:
                        file.modified_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        unmodified_files.append(file)
                    file.file_id = existing_file["dlu_file_id"]
                    existing_files.remove(existing_file)
            query_string = self.insert_dlu_file((file.name, package_id, file.file_id, file.size, file.checksum, file.modified_at, json.dumps(file.metadata)))
            logger.info(query_string)

        return {"files": file_list, "deleted_files": existing_files, "unmodified_files": unmodified_files}

    def get_ready_to_move(self, package_id: str):
        package_record = self.db.get_data(
            "SELECT ready_to_move_from_globus FROM data_manager_data_v WHERE dlu_package_id = %s",
            (package_id,)
        )
        if not len(package_record) == 0:
            return package_record[0]['ready_to_move_from_globus']
        else:
            return "Error: package " + package_id + " not found."

    def find_files_missing_md5(self):
        return self.db.get_data(
            "SELECT * from dlu_file where dlu_md5checksum is NULL"
        )

    def find_all_files(self):
        return self.db.get_data(
            "SELECT * FROM dlu_file"
        )

    def update_md5(self, file_id: str, checksum: str, package_id: str):
        self.db.insert_data("UPDATE dlu_file SET dlu_md5checksum = %s WHERE dlu_file_id = %s and dlu_package_id = %s",
                            (checksum, file_id,package_id))

    def move_globus_files_to_dlu(self, package_id: str):
        ready_status = self.get_ready_to_move(package_id)
        response_msg = "There was an error in marking this package ready to move."
        if ready_status == None:
            validated = self.dlu_file_handler.validate_package_directories(package_id)
            if validated == False:
                response_msg = "Error: directory for package " + package_id + " failed validation."
            else:
                self.update_dlu_package(package_id, {"ready_to_move_from_globus": "yes"})
                response_msg = "Package " + package_id + " successfully marked as ready to move."
        elif ready_status == 'yes':
            response_msg = "Error: package " + package_id + " was already marked as ready to move."
        elif ready_status == 'done':
            response_msg = "Error: package " + package_id + " was already moved."
        else:
            response_msg = ready_status
        return response_msg

    def get_redcapid_by_subjectid(self, subject_id: str):
        result = self.db.get_data(
            "select spectrack_redcap_record_id from spectrack_specimen where spectrack_sample_id = %s", (subject_id,)
        )
        if len(result) > 0:
            return result[0]["spectrack_redcap_record_id"]
        else:
            return None

    def get_biopsy_tracking(self):
        result = self.db.get_data(
            "select * from biopsy_tracking_v"
        )
        return result

    def get_data_manager_data(self):
        result = self.db.get_data(
            """
                select dm.id, dm.dlu_package_id, dm.dlu_created, dm.dlu_submitter, dm.dlu_tis, dm.dlu_packageType, dm.dlu_subject_id, dm.dlu_error, dm.redcap_id, dm.known_specimen, dm.user_package_ready, dm.package_validated, dm.ready_to_move_from_globus, dm.globus_dlu_status, dm.package_status, dm.current_owner, dm.ar_promotion_status, dm.sv_promotion_status, dm.release_version, r.release_date, dm.removed_from_globus, dm.notes 
                from data_manager_data_v dm
                left outer join `release` r on dm.release_version = r.release_version 
            """
        )
        return result

    def get_package(self, package_id: str) -> dict:
        result = self.db.get_data("SELECT * dlu_package_inventory WHERE dlu_package_id = %s", (package_id,))
        if result:
            return result[0]
        else:
            return None

    def get_files_by_package_id(self, package_id: str):
        return self.db.get_data("SELECT * FROM dlu_file WHERE dlu_package_id = %s", (package_id,))

    def delete_files_by_package_id(self, package_id: str):
        return self.db.get_data("DELETE FROM dlu_file WHERE dlu_package_id = %s", (package_id,))

    def get_equal_num_rows(self):
        result = self.db.get_data("SELECT (SELECT COUNT(*) FROM slide_manifest_import) = (SELECT COUNT(*) FROM slide_scan_curation) AS equal_num_rows")
        return result[0]["equal_num_rows"]
    
    def get_new_slide_manifest_import_rows(self):
        return self.db.get_data("SELECT * FROM slide_manifest_import WHERE image_id NOT IN (SELECT image_id FROM slide_scan_curation)")

    def get_spectrack_redcap_record_id(self, kit_id):
        result = self.db.get_data("SELECT spectrack_redcap_record_id FROM spectrack_specimen WHERE spectrack_specimen_kit_id = %s LIMIT 1", (kit_id,))
        if len(result) > 0 and "spectrack_redcap_record_id" in result[0]:
            return result[0]["spectrack_redcap_record_id"]
        else:
            return None

    def insert_into_slide_scan_curation(self, values):
        query = "INSERT INTO slide_scan_curation (image_id, kit_id, redcap_id) VALUES (%s, %s, %s)"
        self.db.insert_data(query, values)
        return query % values

if __name__ == "__main__":
    dlu_management = DluManagement()
    dlu_management.get_data_management_tables()
