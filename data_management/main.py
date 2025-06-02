from services.dlu_management import DluManagement
from services.spectrack_management import SpectrackManagement
from services.tableau import Tableau
from services.redcap import Redcap
import argparse
import logging

logger = logging.getLogger("Main")
logger.setLevel(logging.INFO)


class Main:
    def __init__(self):
        self.dlu_management = DluManagement()
        self.spectrack_management = SpectrackManagement()
        self.tableau = Tableau()

    def import_redcap_data(self):
        dlu_management = DluManagement()
        redcap = Redcap()
        redcap.set_redcap_participant_data()
        redcap_participant_data = redcap.get_redcap_participant_data()
        for redcap_participant in redcap_participant_data:
            dlu_management.insert_redcap_participant(redcap_participant)

    def insert_all_spectrack_specimens(self):
        return self.spectrack_management.insert_all_spectrack_specimens()

    def upsert_new_spectrack_specimens(self):
        return self.spectrack_management.upsert_new_spectrack_specimens()

    def load_biopsy_tracking(self):
        return self.tableau.load_biopsy_tracking()

    def load_data_manager_data(self):
        return self.tableau.load_data_manager_data()


if __name__ == "__main__":
    main = Main()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a",
        "--action",
        choices=["update", "insert"],
        required=True,
        help="update: get new data. insert: insert all data",
    )
    parser.add_argument(
        "-d",
        "--data_source",
        choices=["redcap", "spectrack", "tableau"],
        required=True,
        help="Data source",
    )
    args = parser.parse_args()
    if args.data_source == "spectrack":
        if args.action == "insert":
            records_modified = main.insert_all_spectrack_specimens()
        elif args.action == "update":
            records_modified = main.upsert_new_spectrack_specimens()

    if args.data_source == "redcap":
        if args.action == "insert":
            main.import_redcap_data()

    if args.data_source == "tableau":
        if args.action == "insert" or args.action == "update":
            records_modified = main.load_biopsy_tracking()
            records_modified = records_modified + main.load_data_manager_data()

    if "records_modified" in locals():
        logger.info(f"{records_modified} records modified")
