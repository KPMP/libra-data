import os
import mysql.connector
from dotenv import load_dotenv
import logging
import requests

load_dotenv()
slack_passcode = os.environ.get('slack_passcode')
logger = logging.getLogger("lib-MYSQLConnection")
logging.basicConfig(level=logging.ERROR)
slack_url = "https://hooks.slack.com/services/" + slack_passcode

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

        self.tableau_host = None
        self.tableau_port = None
        self.tableau_user = None
        self.tableau_password = None
        self.tableau_database_name = None

        try:
            self.host = os.environ["mysql_host"]
            self.port = os.environ["mysql_port"]
            self.user = os.environ["mysql_user"]
            self.password = os.environ["mysql_pwd"]
            self.database_name = os.environ["mysql_db"]

            self.tableau_host = os.environ["tableau_mysql_host"]
            self.tableau_port = os.environ["tableau_mysql_port"]
            self.tableau_user = os.environ["tableau_mysql_user"]
            self.tableau_password = os.environ["tableau_mysql_pwd"]
            self.tableau_database_name = os.environ["tableau_mysql_db"]
            self.tableau_ca_file = os.environ["tableau_ca_file"]

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

            self.tableau_host = os.environ.get("tableau_mysql_host")
            self.tableau_port = os.environ.get("tableau_mysql_port")
            self.tableau_user = os.environ.get("tableau_mysql_user")
            self.tableau_password = os.environ.get("tableau_mysql_pwd")
            self.tableau_database_name = os.environ.get("tableau_mysql_db")
            self.tableau_ca_file =  os.environ.get("tableau_ca_file")
        except:
            logger.warning("Can't load environment variables from local .env file")

    def get_db_cursor(self):
        try:
            self.cursor = self.database.cursor(buffered=False, dictionary=True)
            return self.cursor
        except:
            logger.error("Can't get mysql cursor")
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
        except Exception as error:
            logger.error("Can't connect to MySQL: ", exec_info=error)
            os.sys.exit()

    def get_tableau_db_connection(self):
        try:
            self.database = mysql.connector.connect(
                host=self.tableau_host,
                user=self.tableau_user,
                port=self.tableau_port,
                password=self.tableau_password,
                database=self.tableau_database_name,
                ssl={'ca': self.tableau_ca_file}
            )
            self.database.get_warnings = True
            return self.database
        except Exception as error:
            logger.error("Can't connect to MySQL: ", msg=error)
            os.sys.exit()

    def insert_data(self, sql, data):
        try:
            self.get_db_cursor()
            self.cursor.execute(sql, data)
            warning = self.cursor.fetchwarnings()
            if warning is not None:
                print(warning)
        except:
            message = f"Error: Cannot insert with query: {sql}; and the data: {data}"
            logger.error(message)
            requests.post(
                slack_url,
                headers={'Content-type': 'application/json', },
                data='{"text":"' + message + '"}'
            )
        finally:
            self.database.commit()
            self.cursor.close()

    def get_data(self, sql, query_data=None):
        try:
            self.get_db_cursor()
            data = []
            self.cursor.execute(sql, query_data)
            for row in self.cursor:
                data.append(row)
            return data
        except:
            message = "Error: Can't get data_management data."
            logger.error(message)
            requests.post(
                slack_url,
                headers={'Content-type': 'application/json', },
                data='{"text":"' + message + '"}'
            )
        finally:
            self.cursor.close()


if __name__ == "__main__":
    try:
        cursor = self.connection.cursor(buffered=False, dictionary=True)
        logger.info("mysql connection successful, listing available tables")
        cursor.execute("SHOW TABLES;")
        for row in cursor:
            print(row)
    except:
        logger.error("Can't get data_management data")
    finally:
        cursor.close()
