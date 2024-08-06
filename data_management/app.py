from flask import Flask, request
from flask_cors import CORS
from services.dlu_management import DluManagement
from services.dlu_package_inventory import DLUPackageInventory
from services.dlu_utils import dlu_package_dict_to_dpi_tuple, dlu_package_dict_to_dmd_tuple, dlu_file_dict_to_tuple
import logging

logger = logging.getLogger("app")
logger.setLevel(logging.INFO)
app = Flask(__name__)
CORS(app)


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

@app.route("/v1/dlu/package/<package_id>/status", methods=["GET"])
def get_package_status(package_id):
    dlu_package_inventory = DLUPackageInventory()
    dlu_package_inventory.reconnect()
    status = dlu_package_inventory.get_package_status(package_id)
    return status[0]["globus_dlu_status"] if len(status) > 0 and status[0]["globus_dlu_status"] is not None else ""