from lib.mysql_connection import MYSQLConnection
from lib.mongo_connection import MongoConnection
from services.spectrack import SpecTrack
import logging
from services.dlu_filesystem import DLUFileHandler
from services.dlu_mongo import DLUMongo
from services.dlu_state import DLUState
from services.dlu_filesystem import DLUFile
from typing import List
import json

logger = logging.getLogger("services-dataManagement")
logger.setLevel(logging.INFO)


def get_update_query_info(fields_vals: dict):
    query_string = ""
    for field in fields_vals:
        query_string = query_string + field + " = %s, "
    query_obj = {"set_clause": query_string.rstrip(", "), "values": tuple(fields_vals.values())}
    return query_obj


class DataManagement:
    def __init__(self):
        self.db = MYSQLConnection()
        self.db.get_db_connection()
        self.spectrack = SpecTrack()
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

    def get_redcapid_by_subjectid(self, subject_id: str):
        result = self.db.get_data(
            "select spectrack_redcap_record_id from spectrack_specimen where spectrack_sample_id = %s", (subject_id,)
        )
        if len(result) > 0:
            return result[0]["spectrack_redcap_record_id"]
        else:
            return None

    def insert_redcap_participant(self, redcap_participant):
        if self.get_redcap_participant_count(redcap_participant["redcap_id"]) == 0:
            logger.info(
                f"inserting redcap participant with id: {redcap_participant['redcap_id']}"
            )
            self.db.insert_data(
                "INSERT INTO redcap_participant ( "
                + "redcap_id, redcap_np_gender, redcap_age_binned, redcap_tissue_type, "
                + "redcap_protocol, redcap_sample_type, redcap_tissue_source, redcap_clinical_data,"
                  "redcap_exp_aki_kdigo, redcap_exp_race, redcap_exp_alb_cat_most_recent,"
                  "redcap_mh_ht_yn, redcap_mh_diabetes_yn, redcap_exp_has_med_raas, "
                  "redcap_exp_a1c_cat_most_recent, redcap_exp_pro_cat_most_recent, "
                  "redcap_exp_diabetes_duration, redcap_exp_ht_duration, redcap_exp_egfr_bl_cat, redcap_adj_primary_category) "
                + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (list(redcap_participant.values())),
            )
        else:
            logger.warning(
                f"redcap participant with id: {redcap_participant['redcap_id']} already exists, skipping insert"
            )

    def insert_dmd_records_from_spectrack(self, values: tuple):
        self.insert_spectrack_specimen(values)

    def insert_spectrack_specimen(self, values: tuple):
        result = self.db.get_data(
            "SELECT count(spectrack_specimen_id) as specimen_count FROM data_management.spectrack_specimen WHERE spectrack_specimen_id = %s",
            (values[0],),
        )[0]["specimen_count"]
        if result == 0:
            logger.info("Inserting into spectrack_specimen for specimen id: " + str(values[0]))
            self.db.insert_data(
                "INSERT INTO data_management.spectrack_specimen ( "
                + "spectrack_specimen_id, spectrack_sample_id, "
                + "spectrack_sample_type_id, spectrack_sample_type, spectrack_derivative_parent, spectrack_redcap_record_id, "
                + "spectrack_specimen_level,"
                + "spectrack_specimen_type_sample_type_code, spectrack_specimen_kit_id, spectrack_specimen_kit_type_name, "
                + "spectrack_specimen_kit_redcap_project_type, spectrack_specimen_kit_collecting_org, spectrack_biopsy_disease_category, "
                + "spectrack_biopsy_date, spectrack_created_date, spectrack_modified_date) "
                + "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                values,
            )


    def insert_all_spectrack_specimens(self):
        results = self.spectrack.get_specimens(20)
        record_count = self.spectrack.get_next_with_callback(
            results, self.insert_dmd_records_from_spectrack
        )
        return record_count

    def get_spectrack_record(self, specimen_id: int):
        return self.db.get_data(
            "SELECT * FROM data_management.spectrack_specimen WHERE spectrack_specimen_id = "
            + str(specimen_id)
        )

    def get_redcap_id_by_spectrack_sample_id(self, sample_id: str):
        return self.db.get_data(
            "SELECT spectrack_redcap_record_id FROM data_management.spectrack_specimen WHERE spectrack_sample_id = %s",(sample_id,),
        )

    def get_participant_by_redcap_id(self, redcap_id: str):
        return self.db.get_data(
            "SELECT * FROM data_management.redcap_participant WHERE redcap_id = %s",(redcap_id,),
        )

    def get_max_spectrack_date(self):
        result = self.db.get_data(
            "SELECT MAX(spectrack_created_date) as max_date FROM data_management.spectrack_specimen"
        )
        return result[0]["max_date"]

    def update_spectrack_specimen(self, values: tuple):
        # add the specimen ID to the end of the tuple for the WHERE clause
        new_values = values[1:] + (values[0],)
        query = (
                "UPDATE data_management.spectrack_specimen SET spectrack_sample_id = %s, "
                + "spectrack_sample_type_id = %s, spectrack_sample_type = %s, spectrack_derivative_parent = %s, spectrack_redcap_record_id = %s, "
                + "spectrack_specimen_level = %s,"
                + "spectrack_specimen_type_sample_type_code = %s, spectrack_specimen_kit_id = %s, spectrack_specimen_kit_type_name = %s, "
                + "spectrack_specimen_kit_redcap_project_type = %s, spectrack_specimen_kit_collecting_org = %s, spectrack_biopsy_disease_category = %s, "
                + "spectrack_biopsy_date = %s, spectrack_created_date = %s, spectrack_modified_date = %s WHERE spectrack_specimen_id = %s"
        )
        self.db.insert_data(query, new_values)

    def upsert_spectrack_record(self, values: tuple):
        specimen_id = values[0]
        st_record = self.get_spectrack_record(specimen_id)
        if len(st_record) == 0:
            print()
            self.insert_spectrack_specimen(values)
        else:
            self.update_spectrack_specimen(values)

    def upsert_dmd_records_from_spectrack(self, values: tuple):
        self.upsert_spectrack_record(values)

    def upsert_new_spectrack_specimens(self):
        max_date = self.get_max_spectrack_date()
        results = self.spectrack.get_specimens_modified_greater_than(max_date)
        record_count = self.spectrack.get_next_with_callback(
            results, self.upsert_dmd_records_from_spectrack
        )
        return record_count


    def insert_dlu_package(self, dpi_values: tuple, dmd_values: tuple):
        logger.info(f"inserting DLU package with id: {dpi_values[0]}")
        query1 = ("INSERT INTO dlu_package_inventory (dlu_package_id, dlu_created, dlu_submitter, dlu_tis, "
                 + "dlu_packageType, dlu_subject_id, dlu_error, dlu_lfu, globus_dlu_status) "
                 + "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)")
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
        query = "INSERT INTO dlu_file (dlu_fileName, dlu_package_id, dlu_file_id, dlu_filesize, dlu_md5checksum, dlu_metadata) VALUES(%s, %s, %s, %s, %s, %s)"
        self.db.insert_data(query, values)
        return query % values

    def insert_dlu_files(self, package_id: str, file_list: List[DLUFile]):
        logger.info(f"Inserting files for package {package_id}")
        for file in file_list:
            query_string = self.insert_dlu_file((file.name, package_id, file.file_id, file.size, file.checksum, json.dumps(file.metadata)))
            logger.info(query_string)

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


if __name__ == "__main__":
    data_management = DataManagement()
    data_management.get_data_management_tables()
