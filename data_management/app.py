from flask import Flask, request
from flask_cors import CORS
from services.data_management import DataManagement
from services.dlu_package_inventory import DLUPackageInventory
from services.dlu_utils import dlu_package_dict_to_tuple, dlu_file_dict_to_tuple

app = Flask(__name__)
CORS(app)


@app.route("/v1/dlu/package", methods=["POST"])
def add_dlu_package():
    data_management = DataManagement()
    data_management.reconnect()
    content = request.json
    if content["redcapId"] is None:
        content["redcapId"] = data_management.get_redcapid_by_subjectid(content["dluSubjectId"])
        if content["redcapId"] is None and data_management.get_redcap_participant_count(content["dluSubjectId"]) > 0:
            content["redcapId"] = content["dluSubjectId"]

    content_tuple = dlu_package_dict_to_tuple(content)
    data_management.insert_dlu_package(content_tuple)
    return content_tuple[0]

@app.route("/v1/dlu/package/ready", methods=["GET"])
def get_ready_packages():
    dlu_package_inventory = DLUPackageInventory()
    dlu_package_inventory.reconnect()
    files = dlu_package_inventory.get_ready_packages()
    return files

@app.route("/v1/dlu/package/<package_id>", methods=["POST"])
def update_dlu_package(package_id):
    data_management = DataManagement()
    data_management.reconnect()
    content = request.json
    data_management.update_dlu_package(package_id, content)
    return package_id


@app.route("/v1/dlu/file", methods=["POST"])
def add_dlu_file():
    data_management = DataManagement()
    data_management.reconnect()
    content = request.json
    content_tuple = dlu_file_dict_to_tuple(content)
    data_management.insert_dlu_file(content_tuple)
    return content["dluFileId"]


@app.route("/v1/dlu/package/<package_id>/move", methods=["POST"])
def move_dlu_file(package_id):
    data_management = DataManagement()
    data_management.reconnect()
    response = data_management.move_globus_files_to_dlu(package_id)
    return response

@app.route("/v1/dlu/package/<package_id>/status", methods=["GET"])
def get_package_status(package_id):
    dlu_package_inventory = DLUPackageInventory()
    dlu_package_inventory.reconnect()
    status = dlu_package_inventory.get_package_status(package_id)
    return status[0]["globus_dlu_status"] if len(status) > 0 and status[0]["globus_dlu_status"] is not None else ""