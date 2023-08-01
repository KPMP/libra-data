from dlu_package_inventory import DLUPackageInventory
import logging

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
            print(files)        

if __name__ == "__main__":
    DLUWatcher.watch_for_files()