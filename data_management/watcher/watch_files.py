from dlu_package_inventory import DLUPackageInventory
import logging

logger = logging.getLogger("services-dlu_package_watcher")
logger.setLevel(logging.INFO)
     
def watch_for_files():
    files = DLUPackageInventory.get_dlu_file("yes")
    if len(files) == 0:
        logger.info(
            "No records were found with status 'yes' "
        )
    else:
        print(files)        

watch_for_files()