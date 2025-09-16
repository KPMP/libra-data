from flask import Flask, request
from flask_cors import CORS
from services.dlu_management import DluManagement
from services.dlu_package_inventory import DLUPackageInventory
from services.dlu_filesystem import DLUFileHandler, DirectoryInfo
from services.dlu_mongo import DLUMongo
from services.dlu_state import DLUState, PackageState
from lib.mongo_connection import MongoConnection
from services.dlu_utils import dlu_package_dict_to_dpi_tuple, dlu_package_dict_to_dmd_tuple, dlu_file_dict_to_tuple
import logging

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
app = Flask(__name__)
CORS(app)
app.logger.info("For SpecTrack logs, check spectrack.log in the home directory")


@app.route("/v1/dlu/package", methods=["POST"])
def add_dlu_package():
    dlu_management = DluManagement()
    dlu_management.reconnect()
    content = request.json
    if content["redcapId"] is None:
        content["redcapId"] = dlu_management.get_redcapid_by_subjectid(content["dluSubjectId"])
        if content["redcapId"] is None and dlu_management.get_redcap_participant_count(content["dluSubjectId"]) > 0:
            content["redcapId"] = content["dluSubjectId"]

    dpi_content_tuple = dlu_package_dict_to_dpi_tuple(content)
    dmd_content_tuple = dlu_package_dict_to_dmd_tuple(content)
    dlu_management.insert_dlu_package(dpi_content_tuple, dmd_content_tuple)
    return dpi_content_tuple[0]

@app.route("/v1/dlu/package/ready", methods=["GET"])
def get_ready_packages():
    dlu_package_inventory = DLUPackageInventory()
    dlu_package_inventory.reconnect()
    files = dlu_package_inventory.get_ready_packages()
    return files

@app.route("/v1/dlu/package/<package_id>", methods=["POST"])
def update_dlu_package(package_id):
    dlu_management = DluManagement()
    dlu_management.reconnect()
    content = request.json
    dlu_management.update_dlu_package(package_id, content)
    return package_id


@app.route("/v1/dlu/file", methods=["POST"])
def add_dlu_file():
    dlu_management = DluManagement()
    dlu_management.reconnect()
    content = request.json
    content_tuple = dlu_file_dict_to_tuple(content)
    dlu_management.insert_dlu_file(content_tuple)
    return content["dluFileId"]


@app.route("/v1/dlu/package/<package_id>/move", methods=["POST"])
def move_dlu_file(package_id):
    dlu_management = DluManagement()
    dlu_management.reconnect()
    response = dlu_management.move_globus_files_to_dlu(package_id)
    return response

@app.route("/v1/dlu/package/<package_id>/recall", methods=["POST"])
def recall_dlu_package(package_id):
    try:
        dlu_package_inventory = DLUPackageInventory()
        dlu_management = DluManagement()
        dlu_file_handler = DLUFileHandler()
        mongo_connection = MongoConnection().get_mongo_connection()
        dlu_mongo = DLUMongo(mongo_connection)
        dlu_state = DLUState()
        dlu_package_inventory.reconnect()
        dlu_management.reconnect()
        dlu_file_handler.set_recall_package_directories()
    except:
        error_msg = "Error: unable to connect to data manager services. Make sure that the necessary services (Mongo, State Manager, etc.) are running."
        logger.info(error_msg)
        return error_msg

    dlu_data_directory = '/data/package_' + package_id
    directory_info = DirectoryInfo(dlu_data_directory, calculate_checksums = False)
    file_list = None
    if directory_info.file_count == 0 and directory_info.subdir_count == 0:
        error_msg = "Error: package " + package_id + " has no files or top level subdirectory"
        logger.info(error_msg)
        dlu_management.update_dlu_package(package_id, { "globus_dlu_status": error_msg })
        return error_msg
    if directory_info.file_count == 0 and directory_info.subdir_count == 1:
        contents = "".join(directory_info.dir_contents)
        top_level_subdir = package_id + "/" + contents
        file_list = dlu_file_handler.match_files(top_level_subdir,False)
    else:
        file_list = dlu_file_handler.match_files(package_id,False)

    dlu_files = []
    for file in directory_info.file_details:
        file.path = dlu_file_handler.split_path(file.path)['file_path']
        dlu_files.append(file)

    dlu_file_handler.copy_files(package_id, dlu_files)
    dlu_file_handler.chown_dir(package_id, file_list, 99413947)
    dlu_management.update_dlu_package(package_id, { "globus_dlu_status": "recalled" })
    dlu_management.update_dlu_package(package_id, { "ready_to_move_from_globus": None })

    content = request.json
    codicil = content['codicil'] if 'codicil' in content else None
    dlu_state.set_package_state(package_id, PackageState.RECALLED, codicil)
    dlu_state.clear_cache()
    return package_id

@app.route("/v1/dlu/package/<package_id>/status", methods=["GET"])
def get_package_status(package_id):
    dlu_package_inventory = DLUPackageInventory()
    dlu_package_inventory.reconnect()
    status = dlu_package_inventory.get_package_status(package_id)
    return status[0]["globus_dlu_status"] if len(status) > 0 and status[0]["globus_dlu_status"] is not None else ""
