import unittest
from services.redcap import Redcap
from unittest.mock import Mock


class TestRedcap(unittest.TestCase):

    def test_parse_participant_records_no_transform_records(self):
        mock = Mock(Redcap)
        attributes = {"redcap_data": []}
        mock.configure_mock(**attributes)

        result = Redcap.parse_participant_records(mock)
        self.assertEqual([], result)

    def test_parse_participant_records_main_values(self):
        mock = Mock(Redcap)
        attributes = {'parse_redcap_records_by_participant.return_value': 'xx', "redcap_data":
            [{"transform_records": [
                {"field_name": "exp_age_decade", "field_value": "age", "record_id": "123"},
                {"field_name": "exp_aki_kdigo", "field_value": "kdigo", "record_id": "123"},
                {"field_name": "exp_race", "field_value": "race", "record_id": "123"},
                {"field_name": "exp_alb_cat_most_recent", "field_value": "a1b recent", "record_id": "123"},
                {"field_name": "mh_ht_yn", "field_value": "mh_ht y", "record_id": "123"},
                {"field_name": "np_gender", "field_value": "Male", "record_id": "123"},
                {"field_name": "mh_diabetes_yn", "field_value": "diabetes y", "record_id": "123"},
                {"field_name": "exp_has_med_raas", "field_value": "med_raas y", "record_id": "123"},
                {"field_name": "exp_a1c_cat_most_recent", "field_value": "a1c recent", "record_id": "123"},
                {"field_name": "exp_pro_cat_most_recent", "field_value": "pro cat recent", "record_id": "123"},
                {"field_name": "exp_diabetes_duration", "field_value": "200", "record_id": "123"},
                {"field_name": "exp_ht_duration", "field_value": "300", "record_id": "123"},
                {"field_name": "exp_egfr_bl_cat", "field_value": "egfr", "record_id": "123"},
                {"field_name": "exp_disease_type", "field_value": "disease", "record_id": "123"},
            ]}]}
        mock.configure_mock(**attributes)

        result = Redcap.parse_participant_records(mock)
        self.assertEqual([{'redcap_age_binned': 'age',
          'redcap_clinical_data': '',
          'redcap_exp_a1c_cat_most_recent': 'a1c recent',
          'redcap_exp_aki_kdigo': 'kdigo',
          'redcap_exp_alb_cat_most_recent': 'a1b recent',
          'redcap_exp_diabetes_duration': '200',
          'redcap_exp_egfr_bl_cat': 'egfr',
          'redcap_exp_has_med_raas': 'med_raas y',
          'redcap_exp_ht_duration': '300',
          'redcap_exp_pro_cat_most_recent': 'pro cat recent',
          'redcap_exp_race': 'race',
          'redcap_id': '123',
          'redcap_mh_diabetes_yn': 'diabetes y',
          'redcap_mh_ht_yn': 'mh_ht y',
          'redcap_protocol': 'KPMP_MAIN',
          'redcap_sample_type': 'xx',
          'redcap_np_gender': 'Male',
          'redcap_tissue_source': 'KPMP Recruitment Site',
          'redcap_enrollment_category': 'disease'}], result)

    def test_parse_participant_records_other_protocol(self):
        mock = Mock(Redcap)
        attributes = {'parse_redcap_records_by_participant.return_value': 'xx', "redcap_data":
            [{"transform_records": [
                {"field_name": "exp_age_decade", "field_value": "age", "record_id": "123"},

            ], "redcap_project_type": "Side hustle"}]}
        mock.configure_mock(**attributes)

        result = Redcap.parse_participant_records(mock)
        self.assertEqual([{'redcap_age_binned': 'age',
          'redcap_clinical_data': '',
          'redcap_exp_a1c_cat_most_recent': '',
          'redcap_exp_aki_kdigo': '',
          'redcap_exp_alb_cat_most_recent': '',
          'redcap_exp_diabetes_duration': '',
          'redcap_exp_egfr_bl_cat': '',
          'redcap_exp_has_med_raas': '',
          'redcap_exp_ht_duration': '',
          'redcap_exp_pro_cat_most_recent': '',
          'redcap_exp_race': '',
          'redcap_id': '123',
          'redcap_mh_diabetes_yn': '',
          'redcap_mh_ht_yn': '',
          'redcap_protocol': 'Side hustle',
          'redcap_sample_type': 'xx',
          'redcap_np_gender': '',
          'redcap_tissue_source': 'KPMP Recruitment Site',
          'redcap_enrollment_category': ''}], result)

    def test_parse_participant_records_HRT_protocol(self):
        mock = Mock(Redcap)
        attributes = {'get_sample_type_from_tis_mapping.return_value': 'yy'
            , 'parse_redcap_records_by_participant.return_value': 'xx', "redcap_data":
            [{"transform_records": [
                {"field_name": "exp_age_decade", "field_value": "age", "record_id": "123"},

            ], "redcap_project_type": "KPMP_HRT"}]}
        mock.configure_mock(**attributes)

        result = Redcap.parse_participant_records(mock)
        self.assertEqual([{'redcap_age_binned': 'age',
          'redcap_clinical_data': '',
          'redcap_exp_a1c_cat_most_recent': '',
          'redcap_exp_aki_kdigo': '',
          'redcap_exp_alb_cat_most_recent': '',
          'redcap_exp_diabetes_duration': '',
          'redcap_exp_egfr_bl_cat': '',
          'redcap_exp_has_med_raas': '',
          'redcap_exp_ht_duration': '',
          'redcap_exp_pro_cat_most_recent': '',
          'redcap_exp_race': '',
          'redcap_id': '123',
          'redcap_mh_diabetes_yn': '',
          'redcap_mh_ht_yn': '',
          'redcap_protocol': 'KPMP_HRT',
          'redcap_sample_type': 'yy',
          'redcap_np_gender': '',
          'redcap_tissue_source': 'KPMP Recruitment Site',
          'redcap_enrollment_category': 'Healthy Reference'}], result)

if __name__ == '__main__':
    unittest.main()
