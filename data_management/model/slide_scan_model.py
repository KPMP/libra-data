class SlideScanModel:

    def __init__(self, image_id: str, redcap_id: str, kit_id:str, new_file_name: str):
        self.image_id = image_id
        self.redcap_id = redcap_id
        self.kit_id = kit_id
        self.new_file_name = new_file_name

    def get_dmd_tuple(self):
        return(
            self.image_id,
            self.kit_id,
            self.redcap_id,
            self.new_file_name
        )
