from lib.mysql_connection import MYSQLConnection
from services.dlu_management import DluManagement


class Tableau:
    def __init__(self):
        self.db_tableau = MYSQLConnection()
        self.db_tableau.get_tableau_db_connection()

        self.dlu_management = DluManagement()

    def truncate_biopsy_tracking(self):
        result = self.db_tableau.get_data(
            "truncate table biopsy_tracking"
        )
        return result

    def truncate_data_manager_data(self):
        result = self.db_tableau.get_data(
            "truncate table data_manager_data"
        )
        return result

    def load_biopsy_tracking(self):
        self.truncate_biopsy_tracking()
        bt_results = self.dlu_management.get_biopsy_tracking()
        query = "INSERT INTO biopsy_tracking (redcap_record_id, `Whole Slide Images`, `Single-nucleus RNA-Seq Status`, `Single-nucleus RNA-Seq Specimen ID`, `ATAC RNA-seq Status`, `ATAC RNA-seq Specimen ID`, `Single-cell RNA-Seq Status`, `Single-cell RNA-Seq Specimen ID`, `Regional Transcriptomics Status`, `Regional Transcriptomics Specimen ID`, `Bulk total/mRNA Experiment Status`, `Bulk total/mRNA Experiment Specimen ID`, `3D Tissue Imaging and Cytometry Experiment Status`, `3D Tissue Imaging and Cytometry Experiment Specimen ID`, `Regional Proteomics Experiment Status`, `Regional Proteomics Specimen ID`, `Spatial Metabolomics Experiment Status`, `Spatial Metabolomics Specimen ID`, `Spatial Lipidomics Experiment Status`, `Spatial Lipidomics Specimen ID`, `Spatial N-glycomics Experiment Status`, `Spatial N-glycomics Specimen ID`, `Spatial Transcriptomics Experiment Status`, `Spatial Transcriptomics Specimen ID`, `CODEX (IU) Experiment Status`, `CODEX (IU) Specimen ID`, `CODEX (UCSF) Experiment Status`, `CODEX (UCSF) Specimen ID`, `IMC Experiment Status`, `IMC Specimen ID`, `DNA Methyl-seq Experiment Status`, `DNA Methyl-seq Specimen ID`, `CUT & RUN Experiment Status`, `CUT & RUN Specimen ID`, `Metabolon Timed Urine - UHPLC MS-MS Experiment Status`, `Metabolon Timed Urine - UHPLC MS-MS Specimen ID`, `Metabolon Plasma EDTA - UHPLC MS-MS Experiment Status`, `Metabolon Plasma EDTA - UHPLC MS-MS Specimen ID`, `MSDQ120 Spot Urine Biomarker Status`, `MSDQ120 Spot Urine Biomarker Specimen ID`, `MSDQ120 Plasma EDTA Biomarker Status`, `MSDQ120 Plasma EDTA Biomarker Specimen ID`, `Litholink Timed Urine - BCAU680 - Status`, `Litholink Timed Urine - BCAU680 - Specimen ID`, `Stool Microbiome - Qaigen NextEra Status`, `Stool Microbiome - Qaigen NextEra Specimen ID`, `Clinical Chemistry Spot/Timed Urine - BCAU5812 Status`, `Clinical Chemistry Spot/Timed Urine - BCAU5812 Specimen ID`, `Clinical Chemistry Serum - BCAU5812 Status`, `Clinical Chemistry Serum - BCAU5812 Specimen ID`, `SomaScan Plasma EDTA - Status`, `SomaScan Plasma EDTA - Specimen ID`, `SomaScan Spot Urine - Status`, `SomaScan Spot Urine - Specimen ID`, `Descriptor Scoring (TIV)`, `Segmentation/Features Data - Status`, `fMRI - Status`, `Retinal - Status`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        records_modified = 0
        for result in bt_results:
            insert_result = self.db_tableau.insert_data(query, tuple(result.values()))
            records_modified = records_modified + 1
        return records_modified

    def load_data_manager_data(self):
        self.truncate_data_manager_data()
        results = self.dlu_management.get_data_manager_data()
        query = "INSERT INTO kpmp_dvc_integration.data_manager_data(id, dlu_package_id, dlu_created, dlu_submitter, dlu_tis, dlu_packageType, dlu_subject_id, dlu_error, redcap_id, known_specimen, user_package_ready, package_validated, ready_to_move_from_globus, globus_dlu_status, package_status, current_owner, ar_promotion_status, sv_promotion_status, release_version, removed_from_globus, notes) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        records_modified = 0
        for result in results:
            insert_result = self.db_tableau.insert_data(query, tuple(result.values()))
            records_modified = records_modified + 1
        return records_modified





