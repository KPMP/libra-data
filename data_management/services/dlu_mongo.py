import uuid
from lib.mongo_connection import MongoConnection


class DLUMongo:

    def __init__(self, mongo_connection: MongoConnection):
        self.package_collection = mongo_connection.packages

    # Entries in the file list should be a dict with the following fields: name, size, checksum
    def update_package_files(self, package_id: str, files: list):
        mongo_files = []
        for file in files:
            file_id = str(uuid.uuid4())
            mongo_files.append({
                "fileName": file["name"],
                "_id": file_id,
                "size": file["size"],
                "md5Checksum": file["checksum"],
            })
        self.package_collection.update_one({"_id": package_id}, {"$set": {"files": mongo_files, "regenerateZip": True}})
