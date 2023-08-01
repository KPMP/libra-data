import os
import mysql.connector
from dotenv import load_dotenv
import logging

logger = logging.getLogger("connection-MYSQLConnection")
logging.basicConfig(level=logging.ERROR)


class MYSQLConnection:
    def __init__(self):
        logger.debug(
            "Start: MYSQLConnection().__init__(), trying to load environment variables in docker"
        )
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.database_name = None

        try:
            self.host = os.environ["mysql_host"]
            self.port = os.environ["mysql_port"]
            self.user = os.environ["mysql_user"]
            self.password = os.environ["mysql_pwd"]
            self.database_name = os.environ["mysql_db"]
        except:
            logger.warning(
                "Can't load environment variables from docker... trying local .env file instead..."
            )
        try:
            logger.debug(
                "Start: MYSQLConnection().__init__(), trying to load environment variables with local .env file"
            )
            load_dotenv(".env", None, True)
            self.host = os.environ.get("mysql_host")
            self.port = os.environ.get("mysql_port")
            self.user = os.environ.get("mysql_user")
            self.password = os.environ.get("mysql_pwd")
            self.database_name = os.environ.get("mysql_db")
        except:
            logger.warning("Can't load environment variables from local .env file")

    def get_db_cursor(self):
        try:
            self.cursor = self.database.cursor(buffered=False, dictionary=True)
            return self.cursor
        except:
            print("Can't get mysql cursor")
            os.sys.exit()

    def get_db_connection(self):
        try:
            self.database = mysql.connector.connect(
                host=self.host,
                user=self.user,
                port=self.port,
                password=self.password,
                database=self.database_name,
            )
            self.database.get_warnings = True
            return self.database
        except:
            print("Can't connect to MySQL")
            os.sys.exit()

    def insert_data(self, sql, data):
        try:
            self.get_db_cursor()
            self.cursor.execute(sql, data)
            warning = self.cursor.fetchwarnings()
            if warning is not None:
                print(warning)
        except:
            print(f"Cannot insert with query: {sql}; and the data: {data}")
        finally:
            self.database.commit()
            self.cursor.close()

    def get_data(self, sql, query_data=None):
        try:
            data = []
            self.get_db_cursor()
            self.cursor.execute(sql, query_data)
            print("excuted query")
            for row in self.cursor:
                data.append(row)
            print("got data")
            return data
        except:
            print("Can't get data_management data.")
        finally:
            self.cursor.close()


if __name__ == "__main__":
    try:
        cursor = self.connection.cursor(buffered=False)
        print("mysql connection successful, listing available tables")
        cursor.execute("SHOW TABLES;")
        for row in cursor:
            print(row)
    except:
        print("Can't get data_management data")
    finally:
        cursor.close()