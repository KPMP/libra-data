from datetime import datetime

def dlu_package_dict_to_tuple(dlu_inventory: dict):
    # Java timestamp is in milliseconds
    dt_string = datetime.fromtimestamp(dlu_inventory["dluCreated"] / 1000.0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return (
        dlu_inventory["dluPackageId"],
        dt_string,
        dlu_inventory["dluSubmitter"],
        dlu_inventory["dluTis"],
        dlu_inventory["dluPackageType"],
        dlu_inventory["dluSubjectId"],
        dlu_inventory["dluError"],
        dlu_inventory["dluLfu"],
        dlu_inventory["knownSpecimen"],
        dlu_inventory["redcapId"],
        dlu_inventory["userPackageReady"],
        dlu_inventory["packageValidated"],
        dlu_inventory["readyToMoveFromGlobus"],
        dlu_inventory["globusDluStatus"],
        dlu_inventory["removedFromGlobus"],
        None,
        None,
        dlu_inventory["notes"],
    )


def dlu_file_dict_to_tuple(dlu_file: dict):
    return (
        dlu_file["dluFileName"],
        dlu_file["dluPackageId"],
        dlu_file["dluFileId"],
        dlu_file["dluFileSize"],
        dlu_file["dluMd5Checksum"],
    )