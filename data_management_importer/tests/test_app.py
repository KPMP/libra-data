import unittest
from unittest import mock
from app import app

@mock.patch('lib.mysql_connection.MYSQLConnection')
@mock.patch('mysql.connector.connect')
@mock.patch('services.data_management.DataManagement')
class TestApp(unittest.TestCase):


    #NOTE: These don't work
    def test_add_dlu_file(self, mock_datamanagement_constructor, mock_mysqlconnection_constructor, mock_connect):
        mock_data_management = mock_datamanagement_constructor.return_value
        mock_db = mock_mysqlconnection_constructor.return_value
        mock_data_management.db = mock_db
        app.data_manager = mock_data_management
        app.data_manager.db.get_db_connection.return_value = ""
        mock_connect.return_value = ""
        with app.test_client() as client:
            rv = client.post('/v1/dlu/file', json={
                'email': 'flask@example.com', 'password': 'secret'}
                                    )
            json_data = rv.get_json()
            assert json_data == "blah"
