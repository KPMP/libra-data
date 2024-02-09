from lib.mongo_connection import MongoConnection
from services.dlu_filesystem import DLUFile
from typing import List
from enum import Enum
import logging

logger = logging.getLogger("services-DLUMongo")
logging.basicConfig(level=logging.INFO)

EM_IMAGE_TYPE = "EM Images"


class PackageType(Enum):
    ELECTRON_MICROSCOPY = "Electron Microscopy Imaging"
    SEGMENTATION = "Segmentation Masks"
    OTHER = "Other"

class DLUMongo:

    def __init__(self, mongo_connection: MongoConnection):
        self.package_collection = mongo_connection.packages

    # Entries in the file list should be a dict with the following fields: name, size, checksum, and an optional metadata obj
    def update_package_files(self, package_id: str, files: List[DLUFile]) -> int:
        mongo_files = []
        for file in files:
            file_dict = {
                "fileName": file.name,
                "_id": file.file_id,
                "size": file.size,
                "md5Checksum": file.checksum,
            }
            logger.info("Updating " + file.name + " for package " + package_id)
            if len(file.metadata) != 0:
                file_dict["metadata"] = file.metadata
            mongo_files.append(file_dict)
        result = self.package_collection.update_one({"_id": package_id}, {"$set": {"files": mongo_files}})
        return result.modified_count

    def find_by_package_type_and_redcap_id(self, package_type: str, subject_id: str):
        return self.package_collection.find_one({"subjectId": subject_id, "packageType": package_type})

    def add_package(self, package: dict) -> str:
        result = self.package_collection.insert_one(package)
        return result.inserted_id
