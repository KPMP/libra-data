import unittest
from ..dlu_filesystem import dlu_package_dict_to_dpi_tuple, dlu_package_dict_to_dmd_tuple, dlu_file_dict_to_tuple


class Test_dlu(unittest.TestCase):
    def test_dlu_file_dict_to_tuple(self):
        test_data = {
            "dluFileName": "name",
            "dluPackageId": "package_id",
            "dluFileId": "file_id",
            "dluFileSize": 12345,
            "dluMd5Checksum": "checksum",
        }
        output = dlu_file_dict_to_tuple(test_data)
        print(output)
        assert output == ("name", "package_id", "file_id", 12345, "checksum")

    def test_dlu_package_dict_to_dpi_tuple(self):
        test_data = {
            "dluPackageId": "package_id",
            "dluCreated": 1665768333,
            "dluSubmitter": "submitter",
            "dluTis": "tis",
            "dluPackageType": "package_type",
            "dluSubjectId": "subj_id",
            "dluError": True,
            "dluLfu": False,
            "globusDluStatus": "status"
        }
        output = dlu_package_dict_to_dpi_tuple(test_data)
        print(output)
        assert output == ('package_id', '1970-01-20 01:42:48', 'submitter', 'tis', 'package_type', 'subj_id', True, False, None)

    def test_dlu_package_dict_to_dmd_tuple(self):
        test_data = {
            "dluPackageId": "package_id",
            "knownSpecimen": "specimen",
            "redcapId": "redcap",
            "userPackageReady": True,
            "packageValidated": True,
            "readyToMoveFromGlobus": True,
            "removedFromGlobus": False,
            "arPromotionStatus": "promoted-ar",
            "svPromotionStatus": "promoted-sv",
            "notes": "notes"
        }
        output = dlu_package_dict_to_dmd_tuple(test_data)
        print(output)
        assert output == ('package_id', 'redcap', 'specimen', True, True, True, False, 'promoted-ar', 'promoted-sv', 'notes')
