from flask import Flask, request
from flask_cors import CORS
from services.data_management import DataManagement
from services.dlu import dlu_package_dict_to_tuple, dlu_file_dict_to_tuple

app = Flask(__name__)
CORS(app)

@app.route("/v1/dlu/package", methods=["POST"])
def add_dlu_package():
    data_management = DataManagement()
    data_management.reconnect()
    content = request.json
    content_tuple = dlu_package_dict_to_tuple(content)
    data_management.insert_dlu_package(content_tuple)
    return content_tuple[0]


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
    success = data_management.move_globus_files_to_dlu(package_id)
    return {"Success": success}

