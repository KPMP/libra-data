import os
import pymongo
from dotenv import load_dotenv
import logging

logger = logging.getLogger("lib-mongoConnection")
logging.basicConfig(level=logging.ERROR)


class mongoConnection:
    def __init__(self):
        logger.debug(
            "Start: mongoConnection().__init__(), trying to load environment variables in docker"
        )
        self.host = None
        self.database = None

        try:
            self.host = os.environ["mongo_host"]
            self.database = os.environ["mongo_db"]
        except:
            logger.warning(
                f"Can't load environment variables from docker... trying local .env file instead..."
            )
        try:
            logger.debug(
                "Start: mongoConnection().__init__(), trying to load environment variables with local .env file"
            )
            load_dotenv(".env")
            self.host = os.environ.get("mongo_host")
            self.database = os.environ.get("mongo_db")
        except:
            logger.warning(f"Can't load environment variables from local .env file")

    def get_mongo_connection(self):
        try:
            mongo_client = pymongo.MongoClient(
                f"mongodb://{self.host}:27017/", serverSelectionTimeoutMS=5000
            )
            database = mongo_client[self.database]
            return database
        except:
            logger.error(
                f"Can't connect to Mongo\nMake sure you have filled out the correct environment variables in the .env file"
            )
            os.sys.exit()


if __name__ == "__main__":
    database = mongoConnection().get_mongo_connection()
    logger.info("Mongo connection successful, listing available collections")
    print(database.list_collection_names())
