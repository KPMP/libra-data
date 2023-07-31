import os
from lib.mysql_connection import MySQLConnection
import logging
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("dlu_globus_mover")
logger.setLevel(logging.INFO)

db = MySQLConnection()

class DLUPackageInventory:
    def __init__(self):
        self.db = MySQLConnection()
        self.db.get_db_connection()
        
    def reconnect(self):
        self.db = MySQLConnection()
        self.db.get_db_connection()
        
    def get_dlu_file(self, status):
        return self.db.get_data(
            "SELECT * FROM data_management.dlu_package_inventory WHERE ready_to_move_from_globus == %s AND dlu_package_id AND globus_dlu_status == null",
            (status),
        )
    
    def set_dlu_file_waiting(self, status, package_id):
        return self.db.insert_data(
            "UPDATE data_management.dlu_package_inventory WHERE ready_to_move_from_globus_ == %s AND dlu_package_id == %s AND globus_dlu_status == waiting",
            (status, package_id),
        )