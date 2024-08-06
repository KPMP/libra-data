
from lib.mysql_connection import MYSQLConnection
from services.spectrack import SpecTrack
import logging
logger = logging.getLogger("services-spectrackManagement")
logger.setLevel(logging.INFO)

class SpectrackManagement:
    def __init__(self):
        self.spectrack = SpecTrack()
        self.db = MYSQLConnection()
        self.db.get_db_connection()
        
    def reconnect(self):
        self.db = MYSQLConnection()
        self.database = self.db.get_db_connection()
        
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
            self.insert_spectrack_specimen(values)
        else:
            self.update_spectrack_specimen(values)

    def upsert_dmd_records_from_spectrack(self, values: tuple):
        self.upsert_spectrack_record(values)

    def upsert_new_spectrack_specimens(self):
        max_date = self.get_max_spectrack_date()
        results = self.spectrack.get_specimens_modified_greater_than(max_date)
        logger.info("Retrieving specimens modified greater than " + max_date.strftime("%Y-%m-%dT%H:%M:%S"))
        record_count = self.spectrack.get_next_with_callback(
            results, self.upsert_dmd_records_from_spectrack
        )
        return record_count
    
    def get_redcapid_by_subjectid(self, subject_id: str):
        result = self.db.get_data(
            "select spectrack_redcap_record_id from spectrack_specimen where spectrack_sample_id = %s", (subject_id,)
        )
        if len(result) > 0:
            return result[0]["spectrack_redcap_record_id"]
        else:
            return None