from datetime import datetime
from lib.mongo_connection import MongoConnection
from services.dlu_filesystem import DLUFile
from typing import List
from enum import Enum
import logging
from bson.codec_options import CodecOptions

logger = logging.getLogger("services-DLUMongo")
logging.basicConfig(level=logging.INFO)

EM_IMAGE_TYPE = "EM Images"


class PackageType(Enum):
    ELECTRON_MICROSCOPY = "Electron Microscopy Imaging"
    SEGMENTATION = "Segmentation Masks"
    MULTI_MODAL = "Multimodal Mass Spectrometry"
    SINGLE_CELL = "Single-cell RNA-Seq"
    OTHER = "Other"


class DLUMongo:

    def __init__(self, mongo_connection: MongoConnection):
        self.package_collection = mongo_connection.packages.with_options(codec_options=CodecOptions(tz_aware=True))

    def get_modification_info(self, file_info: dict):
        modifications = []
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for file in file_info["files"]:
            if file.modified_at:
                modifications.append("Modified " + file.name + " at " + now)
            else:
                modifications.append("Added " + file.name + " at " + now)
        for file in file_info["deleted_files"]:
            modifications.append("Deleted " + file.name + " at " + now)
        return modifications



    # Entries in the file list should be a dict with the following fields: name, size, checksum, and an optional metadata obj
    def update_package_files(self, package_id: str, file_info: dict) -> int:
        mongo_files = []
        modifications = self.get_modification_info(file_info)
        for file in file_info["files"]:
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
        package = self.find_by_package_id(package_id)
        final_modifications = package["modifications"] + modifications
        result = self.package_collection.update_one({"_id": package_id}, {"$set": {"files": mongo_files, "modifications": final_modifications}})
        return result.modified_count

    def find_by_package_type_and_redcap_id(self, package_type: str, subject_id: str):
        return self.package_collection.find_one({"subjectId": subject_id, "packageType": package_type})

    def find_by_package_id(self, package_id: str):
        return self.package_collection.find_one({"_id": package_id})

    def add_package(self, package: dict) -> str:
        result = self.package_collection.insert_one(package)
        return result.inserted_id

    def find_all_packages_missing_md5s(self):
        return self.package_collection.find({ "files": {"$elemMatch": {"md5Checksum": {"$exists": False}}}})

    # WARNING!!! If you use this method, you MUST call .close() on the returned cursor
    def find_all_packages(self):
        return self.package_collection.find({}, no_cursor_timeout=True, batch_size=1)
