import requests
import logging
import json
import os
from enum import Enum

logger = logging.getLogger("DLUState")

class PackageState(Enum):
    UPLOAD_SUCCEEDED = "UPLOAD_SUCCEEDED"
    METADATA_RECEIVED = "METADATA_RECEIVED"

class DLUState:
    def __init__(self):
        try:
            inside_docker = os.environ["INSIDE_DOCKER"]
        except:
            inside_docker = False
        if inside_docker:
            self.state_url = "http://state-spring:3060/v1/state/host/upload_kpmp_org"
            self.cache_clear_url = "http://orion-spring:3030/v1/clearCache"
        else:
            self.state_url = "http://localhost:3060/v1/state/host/upload_kpmp_org"
            self.cache_clear_url = "http://localhost:3030/v1/clearCache"

    def set_package_state(self, package_id: str, state: PackageState):
        headers = {"Content-type": "application/json", "Accept": "text/plain"}
        data = {
            "packageId": package_id,
            "state": state.value,
            "largeUploadChecked": True
        }
        try:
            requests.post(self.state_url, data=json.dumps(data), headers=headers)
        except requests.exceptions.RequestException as e:
            if e and e.strerror:
                logger.error("There was an error updating the state: " + e.strerror)
            else:
                logger.error("There was an error updating the state.")

    def clear_cache(self):
        requests.get(self.cache_clear_url)
