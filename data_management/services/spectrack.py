from lib.spectrack_connection import SpectrackConnection
from typing import Callable
from dateutil import parser
from datetime import datetime


class SpecTrack:
    def __init__(self):
        self.st = SpectrackConnection()
        self.biopsies = {}
        self.sample_types = {}

    def get_specimens(self, limit: int = 20):
        results = self.st.get_specimens(limit)
        return results

    def get_next_with_callback(self, result_obj: dict, callback: Callable):
        record_count = 0
        page = 1
        while page == 1 or result_obj["next"]:
            if page != 1:
                result_obj = self.st.get_by_url(result_obj["next"])
            results_converted = self.convert_to_dmd_specimens(result_obj["results"])
            for record in results_converted:
                callback(record)
                record_count = record_count + 1
            page = page + 1
        return record_count

    def get_sample_type(self, sample_type_id: int):
        if sample_type_id in self.sample_types:
            sample_type = self.sample_types[sample_type_id]
        else:
            sample_type = self.st.get_specimen_type_by_id(sample_type_id)
            self.sample_types.update({sample_type_id: sample_type})
        return sample_type

    def get_specimen_kit(self, specimen_kit_id: str):
        results = self.st.get_specimen_kit_by_specimen_kit_id(specimen_kit_id)
        return results["results"][0]

    def get_specimen(self, sample_id: str):
        results = self.st.get_specimen_by_sample_id(sample_id)
        return results["results"][0]

    def get_specimens_modified_greater_than(self, last_modified: datetime):
        results = self.st.get_specimens_modified_greater_than(last_modified)
        return results

    def convert_to_dmd_specimens(self, specimens: list):
        dmd_specimens = list()
        for specimen in specimens:
            # We don't want any records without a kit ID or Redcap ID
            if ("specimen_collection__specimen_kit__kit_id" in specimen) and (
                specimen["redcap_record_id"] is not None
            ):
                specimen_kit = self.get_specimen_kit(specimen["specimen_collection__specimen_kit__kit_id"])
                if specimen["specimen_collection__specimen_kit__kit_id"] and "Biopsy Kit" in specimen_kit["kit_type_name"]:
                    site = specimen["specimen_collection__collecting_org__org_name"]
                    disease_category = specimen["disease_category"]
                    biopsy_date = specimen["biopsy_date"]
                else:
                    organization = self.st.get_by_url(specimen_kit["site"])
                    site = organization["org_name"]
                    disease_category = None
                    biopsy_date = None

                sample_type = self.get_sample_type(specimen["sample_type_id"])
                kit_id = "N/A" if specimen['specimen_collection__specimen_kit__kit_id'] is None else specimen['specimen_collection__specimen_kit__kit_id']
                dmd_specimen_tuple = (
                    specimen["id"],
                    specimen["sample_id"],
                    specimen["sample_type_id"],
                    sample_type["child_type_name"],
                    specimen["derivative_parent_id"],
                    specimen["redcap_record_id"],
                    specimen["level"],
                    sample_type["sample_type_code"],
                    kit_id,
                    specimen_kit["kit_type_name"],
                    specimen_kit["redcap_project_type"],
                    site,
                    disease_category,
                    biopsy_date,
                    # e.g. '2019-09-16T15:21:59-04:00'
                    parser.parse(specimen["created"]).strftime("%Y-%m-%d %H:%M:%S"),
                    parser.parse(specimen["modified"]).strftime("%Y-%m-%d %H:%M:%S"),
                )
                dmd_specimens.append(dmd_specimen_tuple)
        return dmd_specimens


if __name__ == "__main__":

    def print_this(text):
        print(text)

    SpecTrack().get_all_specimens_callback(print_this, 100)
