from dlu_management import DluManagement
from ..model.slide_scan_model import SlideScanModel
import logging

logger = logging.getLogger("services-dlu_package_watcher")
logger.setLevel(logging.INFO)


def determine_stain(stain_info, block_id):
    if stain_info == "H&E":
        if block_id != "OCT":
            return "HE"
        elif block_id == "OCT":
            return "FRZ"
    elif stain_info == "TRICHRM":
        return "TRI"
    elif stain_info == "PAS":
        return "PAS"
    elif stain_info == "Toluidine Blue":
        return "TOL"
    elif stain_info == "Jones Methenamine Silver (SIL)":
        return "SIL"
    return None


class SlideManagement:
    def __init__(self, db: DluManagement = None):
        if db:
            self.db = db
        else:
            self.db = DluManagement()

    def process_slide_manifest_imports(self):
        new_records = self.db.get_new_slide_manifest_import_rows()
        for record in new_records:
            kit_id = record["outside_acc"]
            image_id = record["image_id"]
            redcap_id = self.db.get_spectrack_redcap_record_id(kit_id)
            new_file_name = self.determine_new_slide_name(record["barcode_id"], kit_id, record["stain"],
                                                          record["block_id"])
            slide_scan = SlideScanModel(image_id=image_id, redcap_id=redcap_id, kit_id=kit_id,
                                        new_file_name=new_file_name)
            self.db.insert_into_slide_scan_curation(slide_scan.get_dmd_tuple())

            # If we were unable to determine the filename, we want to update this with an error message, after a
            # record is in slide_scan_curation
            if new_file_name is None:
                self.db.set_error_message_slide_scan_curation("Unknown stain type", image_id=image_id)

    def determine_new_slide_name(self, sample_id: str, kit_id: str, stain_info: str, block_id: str):
        slides_for_kit = self.db.get_slide_manifest_import_by_kit(kit_id, stain_info)
        denominator = len(slides_for_kit)
        numerator = 1
        
        # Keep counting until we find this slide
        for slide in slides_for_kit:
            if slide['barcode_id'] != sample_id:
                numerator = numerator + 1
        stain_type = determine_stain(stain_info, block_id)

        # If we are unable to determine the stain type, we will leave the new filename blank
        if stain_type is None:
            logger.info("Unable to determine stain type from stain: " + stain_info + " and block_id: " + block_id)
            return None
        else:
            return sample_id + "_" + stain_type + "_" + str(numerator) + "of" + str(denominator)