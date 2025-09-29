from lib.mongo_connection import MongoConnection
from bson.objectid import ObjectId
import os
import datetime
import logging

logger = logging.getLogger("services-redcap")
logger.setLevel(logging.INFO)


class Redcap:
    def __init__(self):
        logger.debug("Start: Redcap().__init__")
        self.collection = MongoConnection().get_mongo_connection().redcap
        self.redcap_data = []
        self.participant_data = []
        logger.debug("End: Redcap().__init__")

    def set_redcap_participant_data(self):
        logger.debug("Start: set_redcap_participant_data")
        self.get_redcap_participant_records(
            self.get_latest_batch_of_redcap_object_ids()
        )
        self.participant_data = self.parse_participant_records()
        self.verify_expected_redcap_participant_data_is_present()
        logger.debug("End: set_redcap_participant_data")

    def get_redcap_participant_data(self):
        logger.debug(f"Start: get_redcap_participant_data: {self.participant_data}")
        return self.participant_data

    # We want to be alerted if any of the expected data on a participant is missing
    def verify_expected_redcap_participant_data_is_present(self):
        logger.debug("Start: verify_expected_redcap_participant_data_is_present")
        participant_object = {
            "redcap_id": "",
            "redcap_np_gender": "",
            "redcap_age_binned": "",
            "redcap_enrollment_category": "",
            "redcap_protocol": "",
            "redcap_sample_type": "",
            "redcap_tissue_source": "",
        }
        for participant in self.participant_data:
            for key in participant_object.keys():
                if participant[key] == "" or participant[key] == None:
                    logger.error(
                        f"Error: expected data {key} is missing on a participant: {participant}."
                    )
        logger.debug("End: verify_expected_redcap_participant_data_is_present")

    # The redcap data is exported from redcap into mongodb in batches, with each batch containing the complete set of participant data.
    # Due to the large size of these batches, they must be chunked before the data is stored in mongo.
    # This function returns the objectID of the latest batch of redcap data using the date to find all of the associated chunks.
    def get_latest_batch_of_redcap_object_ids(self):
        last_insert = self.collection.find({}).limit(1).sort("_id", -1)
        last_insert_date = list(last_insert)[0]["created_at"]
        object_ids = self.collection.find(
            {
                "created_at": {
                    "$gte": datetime.datetime.strptime(
                        last_insert_date.strftime("%Y-%m-%d"), "%Y-%m-%d"
                    )
                }
            }
        ).distinct("_id")
        return object_ids

    # it is far from optimal to have to loop through all records to get the value for a single field
    # but this is easier/less bug prone to write than merging the two queries.
    # If performance becomes an issue, this is a good place to start optimizing
    def parse_redcap_records_by_participant(self, redcap_id, field_name):
        logger.debug("Start: parse_redcap_records_by_participant")
        redcap_chunk_ids = []
        for redcap_chunk in self.redcap_data:

            for record in redcap_chunk["redcap_records"]:
                if record["field_name"] == field_name and "record_id" in record and record["record_id"] == redcap_id:
                    if record["value"] == "1":
                        return "Percutaneous Needle Biopsy"
                    elif record["value"] == "2":
                        return "Intra-operative Needle Biopsy"
                    else:
                        logger.error(
                            f'Error: unknown value for record with field_name: {record["field_name"]} value: {record["value"]}'
                        )
                        os.sys.exit()
        logger.debug("End: parse_redcap_records_by_participant")
        return ""

    # HRT is a special case, we can't get sample_type from redcap directly, but we can get the value based off of the TIS location
    # this could lead to issues in the future if the TIS starts to do multiple types of biopsies, but for now it is fine
    # we can get the TIS location from the first 3 characters of the redcap_id
    def get_sample_type_from_tis_mapping(self, participant):
        if participant['redcap_id'][0:3] == '163': # Indiana
            return 'Intra-operative Biopsy'

        if participant['redcap_id'][0:3] == '164': # UCSF
            return 'Transplant Pre-perfusion Biopsy'

        if participant['redcap_id'][0:3] == '165': # Cincinnati
            return 'Transplant Pre-perfusion Biopsy'


    def parse_participant_records(self):
        participant_records = []
        for redcap_chunk in self.redcap_data:
            if (
                "transform_records" in redcap_chunk.keys()
                and len(redcap_chunk["transform_records"]) > 0
            ):
                for record in redcap_chunk["transform_records"]:
                    participant = None
                    existing_record_found = False

                    for i, x in enumerate(participant_records):
                        if x["redcap_id"] == record["record_id"]:
                            existing_record_found = True
                            participant = x

                    if participant is None:
                        participant = {
                            "redcap_id": record["record_id"],
                            "redcap_np_gender": "",
                            "redcap_age_binned": "",
                            "redcap_enrollment_category": "",
                            "redcap_protocol": "",
                            "redcap_sample_type": self.parse_redcap_records_by_participant(
                                record["record_id"], "bp_type"
                            ),
                            "redcap_tissue_source": "",
                            "redcap_clinical_data": "",
                            "redcap_exp_aki_kdigo": "",
                            "redcap_exp_race": "",
                            "redcap_exp_alb_cat_most_recent": "",
                            "redcap_mh_ht_yn": "",
                            "redcap_mh_diabetes_yn": "",
                            "redcap_exp_has_med_raas": "",
                            "redcap_exp_a1c_cat_most_recent": "",
                            "redcap_exp_pro_cat_most_recent": "",
                            "redcap_exp_diabetes_duration": "",
                            "redcap_exp_ht_duration": "",
                            "redcap_exp_egfr_bl_cat": "",
                            "adj_primary_category": "",
                            "redcap_exp_egfr_bl": ""
                        }

                    if record["field_name"] == "np_gender":
                        participant["redcap_np_gender"] = record["field_value"]

                    elif record["field_name"] == "exp_age_decade":
                        participant["redcap_age_binned"] = record["field_value"]

                    elif record["field_name"] == "exp_aki_kdigo":
                        participant["redcap_exp_aki_kdigo"] = record["field_value"]

                    elif record["field_name"] == "exp_race":
                        participant["redcap_exp_race"] = record["field_value"]

                    elif record["field_name"] == "exp_alb_cat_most_recent":
                        participant["redcap_exp_alb_cat_most_recent"] = record["field_value"]

                    elif record["field_name"] == "mh_ht_yn":
                        participant["redcap_mh_ht_yn"] = record["field_value"]

                    elif record["field_name"] == "mh_diabetes_yn":
                        participant["redcap_mh_diabetes_yn"] = record["field_value"]

                    elif record["field_name"] == "exp_has_med_raas":
                        participant["redcap_exp_has_med_raas"] = record["field_value"]

                    elif record["field_name"] == "exp_a1c_cat_most_recent":
                        participant["redcap_exp_a1c_cat_most_recent"] = record["field_value"]

                    elif record["field_name"] == "exp_pro_cat_most_recent":
                        participant["redcap_exp_pro_cat_most_recent"] = record["field_value"]

                    elif record["field_name"] == "exp_diabetes_duration":
                        participant["redcap_exp_diabetes_duration"] = record["field_value"]

                    elif record["field_name"] == "exp_ht_duration":
                        participant["redcap_exp_ht_duration"] = record["field_value"]

                    elif record["field_name"] == "exp_egfr_bl_cat":
                        participant["redcap_exp_egfr_bl_cat"] = record["field_value"]

                    elif record["field_name"] == "exp_disease_type":
                        participant["redcap_enrollment_category"] = record["field_value"]

                    elif record["field_name"] == "adj_primary_categoryC":
                        participant["adj_primary_category"] = record["field_value"]
                        
                    elif record["field_name"] == "exp_egfr_bl":
                        participant["redcap_exp_egfr_bl"] = record["field_value"]    

                    else:
                        logger.error(
                            f'Error: Additional fields found we are not mapping: record with field_name: {record["field_name"]}'
                        )

                    participant["redcap_tissue_source"] = "KPMP Recruitment Site"  # hard-coded value provided by Jonas

                    if redcap_chunk["redcap_project_type"] != "KPMP_MAIN":
                        print(redcap_chunk["redcap_project_type"])

                    if "redcap_project_type" in redcap_chunk:
                        participant["redcap_protocol"] = redcap_chunk["redcap_project_type"]
                    else:  # default to KPMP_MAIN as old data does not have a redcap_project_type
                        participant["redcap_protocol"] = "KPMP_MAIN"

                    if participant["redcap_protocol"] == "KPMP_HRT":
                        participant["redcap_enrollment_category"] = "Healthy Reference"
                        participant["redcap_sample_type"] = self.get_sample_type_from_tis_mapping(participant)
                        participant["redcap_tissue_source"] = "KPMP Opportunity Pool HRT Site"



                    if not existing_record_found:
                        participant_records.append(participant)

        return participant_records

    def get_redcap_participant_records(self, object_ids):
        logger.debug("Start: get_redcap_participant_records")

        for object_id_to_search in object_ids:
            self.redcap_data.append(
                self.collection.find_one({"_id": ObjectId(object_id_to_search)})
            )

        logger.debug("End: get_redcap_participant_records")


if __name__ == "__main__":
    redcap = Redcap()
    redcap_data = redcap.get_redcap_data()
