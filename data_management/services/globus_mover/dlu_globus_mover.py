import os
from lib.mysql_connection import MYSQLConnection
import logging
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("dlu_globus_mover")
logger.setLevel(logging.INFO)

db = MYSQLConnection()

class DLUGlobusWatcher:
    def __init__(self):
        self.db = MYSQLConnection()
        self.db.get_db_connection()
        
    def reconnect(self):
        self.db = MYSQLConnection()
        self.db.get_db_connection()
        
    def get_dlu_file_by_status(self):
        return self.db.get_data(
            "SELECT * FROM data_management.dlu_package_inventory WHERE ready_to_move_from_globus == %s AND globus_dlu_status == "
        )