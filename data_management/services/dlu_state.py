import requests
import logging
import json

logger = logging.getLogger("DLUState")


class DLUState:
    def __init__(self):
        self.state_url = "http://state-spring:3060/v1/state/host/upload_kpmp_org"
        self.cache_clear_url = "http://orion-spring:3030/v1/clearCache"

    def set_package_upload_success(self, package_id: str):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        data = {
            "packageId": package_id,
            "state": "UPLOAD_SUCCEEDED",
            "largeUploadChecked": True
        }
        try:
            requests.post(self.state_url, data=json.dumps(data), headers=headers)
        except requests.exceptions.RequestException as e:
            logger.error("There was an error updating the state: " + e.strerror)
        self.clear_cache()

    def clear_cache(self):
        requests.get(self.cache_clear_url)
