import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
from datetime import datetime
import logging

logger = logging.getLogger("lib-SpectrackConnection")
logging.basicConfig(level=logging.ERROR)

class SpectrackConnection:

    SPECIMEN_URL_SUFFIX = "/specimens"
    SPECIMEN_TYPE_URL_SUFFIX = "/specimen_types"
    SPECIMEN_KIT_URL_SUFFIX = "/specimen_kits"
    BIOPSY_URL_SUFFIX = "/biopsies"
    ORGANIZATION_URL_SUFFIX = "/organizations"
    base_params = {"format": "json"}

    def __init__(self):
        self.token = os.environ["spectrack_token"]
        if not self.token:
            logger.error("The Spectrack token is empty")
        self.spectrack_base_url = os.environ["spectrack_base_url"]
        if not self.spectrack_base_url:
            logger.error("No Spectrack URL provided")
        self.headers = {"Authorization": "Token " + self.token}
        self.session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.check_connection()

    def get_url(self, url_suffix: str):
        return self.spectrack_base_url + url_suffix

    def check_connection(self):
        res = requests.post(
            self.get_url(self.SPECIMEN_URL_SUFFIX),
            {"format": "json", "limit": "20"},
            headers=self.headers,
        )
        print(res.raise_for_status())

    def get_specimens(self, limit: int):
        return self.get_results(
            self.get_url(self.SPECIMEN_URL_SUFFIX), {"limit": limit}
        )

    def get_specimens_modified_greater_than(self, last_modified: datetime):
        reformatted_date = last_modified.strftime("%Y-%m-%dT%H:%M:%S")
        return self.get_results(
            self.get_url(self.SPECIMEN_URL_SUFFIX), {"modified__gte": reformatted_date}
        )

    def get_by_url(self, url: str):
        return self.get_results(url)

    def get_specimen_by_sample_id(self, sample_id: str):
        return self.get_results(
            self.get_url(self.SPECIMEN_URL_SUFFIX), {"sample_id": sample_id}
        )

    def get_specimen_type_by_id(self, specimen_type_id: int):
        return self.get_results(
            self.get_url(self.SPECIMEN_TYPE_URL_SUFFIX) + "/" + str(specimen_type_id)
        )

    def get_specimen_kit_by_specimen_kit_id(self, specimen_kit_id: str):
        return self.get_results(
            self.get_url(self.SPECIMEN_KIT_URL_SUFFIX), {"kit_id": specimen_kit_id}
        )

    def get_biopsy_by_redcap_id(self, redcap_id: str):
        return self.get_results(
            self.get_url(self.BIOPSY_URL_SUFFIX), {"redcap_record_id": redcap_id}
        )

    def get_organization_by_id(self, org_id: int):
        return self.get_results(
            self.get_url(self.ORGANIZATION_URL_SUFFIX) + "/" + str(org_id)
        )

    def get_results(self, url: str, params: dict = {}):
        params.update(self.base_params)
        if "?" in url:  # Don't use params if they're already in the URL
            params = {}
        res = self.session.get(url, params=params, headers=self.headers)
        try:
            res_json = res.json()
        except json.decoder.JSONDecodeError as error:
            print(
                "There was a problem decoding the JSON from: "
                + url
                + " with params "
                + params
            )
        return res_json


if __name__ == "__main__":
    SpectrackConnection().check_connection()
    print("Specktrack connection successful.")
