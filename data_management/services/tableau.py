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

    def print_biopsy_tracking(self):
        bt_results = self.dlu_management.get_biopsy_tracking()
        for result in bt_results:
           print(result)

    def load_biopsy_tracking(self):
        self.truncate_biopsy_tracking()
        bt_results = self.dlu_management.get_biopsy_tracking()
        query = "INSERT INTO data_management.biopsy_tracking_test_v (spectrack_redcap_record_id, 'Whole Slide Images', 'Single-nucleus RNA-Seq Status', 'Single-nucleus RNA-Seq Specimen ID', 'ATAC RNA-seq Status', 'ATAC RNA-seq Specimen ID', 'Single-cell RNA-Seq Status', 'Single-cell RNA-Seq Specimen ID', 'Regional Transcriptomics Status', 'Regional Transcriptomics Specimen ID', 'Bulk total/mRNA Experiment Status', 'Bulk total/mRNA Experiment Specimen ID', '3D Tissue Imaging and Cytometry Experiment Status', '3D Tissue Imaging and Cytometry Experiment Specimen ID', 'Regional Proteomics Experiment Status', 'Regional Proteomics Specimen ID', 'Spatial Metabolomics Experiment Status', 'Spatial Metabolomics Specimen ID', 'Spatial Lipidomics Experiment Status', 'Spatial Lipidomics Specimen ID', 'Spatial N-glycomics Experiment Status', 'Spatial N-glycomics Specimen ID', 'Spatial Transcriptomics Experiment Status', 'Spatial Transcriptomics Specimen ID', 'CODEX (IU) Experiment Status', 'CODEX (IU) Specimen ID', 'CODEX (UCSF) Experiment Status', 'CODEX (UCSF) Specimen ID', 'IMC Experiment Status', 'IMC Specimen ID', 'DNA Methyl-seq Experiment Status', 'DNA Methyl-seq Specimen ID', 'CUT & RUN Experiment Status', 'CUT & RUN Specimen ID', 'Metabolon Timed Urine - UHPLC MS-MS Experiment Status', 'Metabolon Timed Urine - UHPLC MS-MS Specimen ID', 'Metabolon Plasma EDTA - UHPLC MS-MS Experiment Status', 'Metabolon Plasma EDTA - UHPLC MS-MS Specimen ID') VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)";
        for result in bt_results:
            insert_result = self.db_tableau.insert_data(query, result)



