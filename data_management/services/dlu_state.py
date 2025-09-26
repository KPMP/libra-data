import requests
import logging
import json
import os
from enum import Enum

logger = logging.getLogger("DLUState")

class PackageState(Enum):
    UPLOAD_SUCCEEDED = "UPLOAD_SUCCEEDED"
    METADATA_RECEIVED = "METADATA_RECEIVED"
    RECALLED = "RECALLED"

class DLUState:
    def __init__(self):
        host_name = 'localhost'
        try:
            inside_docker = os.environ["INSIDE_DOCKER"]
            host_name = os.environ["dlu_hostname_with_underscores"]
        except:
            inside_docker = False
        if inside_docker:
            self.state_url = "http://state-spring:3060/v1/state/host/" + host_name
            self.cache_clear_url = "http://orion-spring:3030/v1/clearCache"
        else:
            self.state_url = "http://localhost:3060/v1/state/host/" + host_name
            self.cache_clear_url = "http://localhost:3030/api/v1/clearCache"

    def set_package_state(self, package_id: str, state: PackageState, codicil = None):
        headers = {"Content-type": "application/json", "Accept": "text/plain"}
        data = {
            "packageId": package_id,
            "state": state.value,
            "largeUploadChecked": True
        }
        if codicil:
            data["codicil"] = codicil
        try:
            requests.post(self.state_url, data=json.dumps(data), headers=headers)
        except requests.exceptions.RequestException as e:
            if e and e.strerror:
                logger.error("There was an error updating the state: " + e.strerror)
            else:
                logger.exception("There was a problem updating state", e)

    def clear_cache(self):
        requests.get(self.cache_clear_url)
