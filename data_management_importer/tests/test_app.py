import unittest
from unittest import mock
from app import app


@mock.patch('lib.mysql_connection.MYSQLConnection')
@mock.patch('mysql.connector.connect')
class TestApp(unittest.TestCase):

    def test_add_dlu_file(self, mock_connect, mock_mysqlconnection):
        with app.test_client() as client:
            rv = client.post('/v1/dlu/file', json={
                'dluFileName': 'file_name', 'dluPackageId': 'package_id',
                'dluFileId': "file_id", 'dluFileSize': 1235, 'dluMd5Checksum': 'checksum'}
                             )
            assert rv.data.decode() == "file_id"

    def test_add_dlu_package(self, mock_connect, mock_mysqlconnection):
        with app.test_client() as client:
            rv = client.post('/v1/dlu/package', json={"dluPackageId": "packageid",
                                                      "dluCreated": 1667572308,
                                                      "dluSubmitter": "submitter",
                                                      "dluTis": "tis",
                                                      "dluPackageType": "pt",
                                                      "dluSubjectId": "subject",
                                                      "dluError": 0,
                                                      "dluLfu": 1,
                                                      "knownSpecimen": "speciment",
                                                      "redcapId": "redcap",
                                                      "userPackageReady": 1,
                                                      "dvcValidationComplete": 1,
                                                      "packageValidated": 1,
                                                      "readyToPromoteDlu": 0,
                                                      "promotionDluSucceeded": 1,
                                                      "removedFromGlobus": 1,
                                                      "promotionStatus": "promotion",
                                                      "notes": "notes"}
                             )
            assert rv.data.decode() == "packageid"

    def test_update_dlu_package(self, mock_connect, mock_mysqlconnection):
        with app.test_client() as client:
            rv = client.post('/v1/dlu/package/packageid', json={"dluPackageId": "packageid",
                                                            "dluCreated": "created",
                                                            "dluSubmitter": "submitter",
                                                            "dluTis": "tis",
                                                            "dluPackageType": "pt",
                                                            "dluSubjectId": "subject",
                                                            "dluError": 0,
                                                            "dluLfu": 1,
                                                            "knownSpecimen": "speciment",
                                                            "redcapId": "redcap",
                                                            "userPackageReady": 1,
                                                            "dvcValidationComplete": 1,
                                                            "packageValidated": 1,
                                                            "readyToPromoteDlu": 0,
                                                            "promotionDluSucceeded": 1,
                                                            "removedFromGlobus": 1,
                                                            "promotionStatus": "promotion",
                                                            "notes": "notes"}
                             )
            assert rv.data.decode() == "packageid"
