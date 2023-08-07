from dlu_package_inventory import DLUPackageInventory
import logging
import time

logger = logging.getLogger("services-dlu_package_watcher")
logger.setLevel(logging.INFO)
  
class DLUWatcher:   
    def __init__ (self, db: DLUPackageInventory = None):
        if db:
            self.db = db
        else:
            self.db = DLUPackageInventory()
    
    def watch_for_files(self):
        files = self.db.get_dlu_file("yes")
        if len(files) == 0:
            logger.info(
                "No records were found with status 'yes' "
            )
        else:
            self.update_files_for_globus(files)
            
    def update_files_for_globus(self, files):
        for file_result in files:
            logger.info("Setting file status to 'waiting' on package " + str(file_result['dlu_package_id']))
            self.db.set_dlu_file_waiting("yes", file_result['dlu_package_id'])
    

if __name__ == "__main__":
    while True:
        dlu_watcher = DLUWatcher()
        dlu_watcher.watch_for_files()
        time.sleep(10*60)