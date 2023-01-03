import unittest
from unittest import mock

from ..data_management import DataManagement


@mock.patch('lib.mysql_connection.MYSQLConnection')
@mock.patch('mysql.connector.connect')
class TestDataManagement(unittest.TestCase):

    def test_insert_dlu_package(self, mock_mysqlconnection_constructor, mock_connect):
        mock_db = mock_mysqlconnection_constructor.return_value
        mock_db.get_db_connection.return_value = ""
        mock_connect.return_value = ""
        dm = DataManagement()
        data = ('package_id', '1970-01-20 01:42:48', 'submitter', 'tis', 'package_type', 'subj_id', True, False, 'specimen', 'redcap', True, True, True, True, False, False, 'promoted', 'notes')
        output = dm.insert_dlu_package(data)
        assert output == "INSERT INTO dlu_package_inventory (dlu_package_id, dlu_created, dlu_submitter, dlu_tis, dlu_packageType, dlu_subject_id, dlu_error, dlu_lfu, known_specimen, redcap_id, user_package_ready, dvc_validation_complete, package_validated, ready_to_promote_dlu, promotion_dlu_succeeded, removed_from_globus, promotion_status, notes) VALUES(package_id, 1970-01-20 01:42:48, submitter, tis, package_type, subj_id, True, False, specimen, redcap, True, True, True, True, False, False, promoted, notes)"

    def test_insert_dlu_file(self, mock_mysqlconnection_constructor, mock_connect):
        mock_db = mock_mysqlconnection_constructor.return_value
        mock_db.get_db_connection.return_value = ''
        mock_connect.return_value = ""
        dm = DataManagement()
        data = ('name', 'package_id', 'file_id', 12345, 'checksum')
        output = dm.insert_dlu_file(data)
        assert output == "INSERT INTO dlu_file (dlu_fileName, dlu_package_id, dlu_file_id, dlu_filesize, dlu_md5checksum) VALUES(name, package_id, file_id, 12345, checksum)"