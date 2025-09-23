import logging

logger = logging.getLogger("services-dlu_package_watcher")
logger.setLevel(logging.INFO)


class SlideScanModel:

    def __init__(self, image_id: str, redcap_id: str, kit_id: str, new_file_name: str):
        self.image_id = image_id
        self.redcap_id = redcap_id
        self.kit_id = kit_id
        self.new_file_name = new_file_name

    def get_dmd_tuple(self):
        return (
            self.image_id,
            self.kit_id,
            self.redcap_id,
            self.new_file_name
        )


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

def calculate_denominator(slides_for_kit, block_id):
    denominator = 0
    for slide in slides_for_kit:
        if block_id != 'OCT' and slide['block_id'] != 'OCT':
            denominator = denominator + 1
        elif block_id == 'OCT' and slide['block_id'] == 'OCT':
            denominator = denominator + 1
    return denominator


def calculate_numerator(block_id, sample_id, slides_for_kit):
    numerator = 1
    # Keep counting until we find this slide
    for slide in slides_for_kit:
        if slide['barcode_id'] != sample_id:
            if block_id != 'OCT' and slide['block_id'] != 'OCT':
                numerator = numerator + 1
            elif block_id == 'OCT' and slide['block_id'] == 'OCT':
                numerator = numerator + 1
        else:
            break
    return numerator


class SlideManagement:
    def __init__(self, db):
        self.db = db

    def process_slide_manifest_imports(self):
        new_records = self.db.get_new_slide_manifest_import_rows()
        for record in new_records:
            kit_id = record["outside_acc"]
            image_id = record["image_id"]
            redcap_id = self.db.get_spectrack_redcap_record_id(kit_id)
            new_file_name = self.determine_new_slide_name(sample_id=record["barcode_id"], kit_id=kit_id,
                                                          stain_info=record["stain"], block_id=record["block_id"])
            slide_scan = SlideScanModel(image_id=image_id, redcap_id=redcap_id, kit_id=kit_id,
                                        new_file_name=new_file_name)
            self.db.insert_into_slide_scan_curation(slide_scan.get_dmd_tuple())

            # If we were unable to determine the filename, we want to update this with an error message, after a
            # record is in slide_scan_curation
            if new_file_name is None:
                self.db.set_error_message_slide_scan_curation("Unknown stain type", image_id=image_id)

    def determine_new_slide_name(self, sample_id: str, kit_id: str, stain_info: str, block_id: str):
        slides_for_kit = self.db.get_slide_manifest_import_by_kit(kit_id, stain_info)

        denominator = calculate_denominator(slides_for_kit, block_id)

        numerator = calculate_numerator(block_id, sample_id, slides_for_kit)

        stain_type = determine_stain(stain_info, block_id)
        # If we are unable to determine the stain type, we will leave the new filename blank
        if stain_type is None:
            logger.info("Unable to determine stain type from stain: " + stain_info + " and block_id: " + block_id)
            return None
        else:
            return sample_id + "_" + stain_type + "_" + str(numerator) + "of" + str(denominator) + ".svs"
