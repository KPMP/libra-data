import os
from lib.mysql_connection import MYSQLConnection
import logging
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("dlu_globus_mover")
logger.setLevel(logging.INFO)


class DLUPackageInventory:
    def __init__(self):
        self.db = MYSQLConnection()
        self.db.get_db_connection()
        
    def reconnect(self):
        self.db = MYSQLConnection()
        self.database = self.db.get_db_connection()
        
    def get_dlu_file(self, status):
        return self.db.get_data(
            'SELECT * FROM data_management.data_manager_data_v WHERE ready_to_move_from_globus = %s AND (globus_dlu_status IS NULL OR globus_dlu_status = "recalled")',
            (status,)
        )
    
    def set_dlu_file_waiting(self, status, package_id):
        return self.db.insert_data(
            'UPDATE data_management.data_manager_data_v SET globus_dlu_status = "waiting" WHERE ready_to_move_from_globus = %s AND dlu_package_id = %s',
            (status, package_id,)
        )
        
    def get_waiting_files(self):
        return self.db.get_data(
            'Select * from data_management.data_manager_data_v where globus_dlu_status = "waiting" and ready_to_move_from_globus = "yes"'
        )
    
    def get_ready_packages(self):
        return self.db.get_data(
            'SELECT dlu_package_id, globus_dlu_status FROM data_management.data_manager_data_v WHERE ready_to_move_from_globus = "yes"'
        )

    def get_package_status(self, package_id):
        return self.db.get_data(
            'SELECT globus_dlu_status FROM data_management.data_manager_data_v WHERE dlu_package_id = %s',
            (package_id,)
        )

    def get_processing_packages(self):
        return self.db.get_data(
            'SELECT dlu_package_id FROM data_management.data_manager_data_v WHERE globus_dlu_status = "processing"'
        )