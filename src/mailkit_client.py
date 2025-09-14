import logging
from dataclasses import dataclass

import requests
from keboola.component.exceptions import UserException

from configuration import Dataset

ENDPOINT = "https://api.mailkit.eu/json.fcgi"


@dataclass
class MailkitClient:
    client_id: str
    client_md5: str

    def _call_api(self, ds: Dataset, payload: dict) -> list | None:
        payload.update(
            {
                "function": ds.api_function,
                "id": self.client_id,
                "md5": self.client_md5,
            }
        )

        try:
            resp = requests.post(ENDPOINT, json=payload)
            logging.debug("Mailkit API response: HTTP %i %s", resp.status_code, resp.reason)
            logging.debug("Response body: %s", resp.text)
            if resp.status_code != 200:
                api_call = payload["function"]
                raise UserException(f"{api_call} error: {resp.text}")

            result = resp.json()
            if result:
                logging.info("Getting %s: OK", ds.description)
            return result
        except Exception as e:
            logging.exception("Failed to get %s: %s", ds.description, e)

        return None

    def summary_report(self, ds: Dataset, date_from: str, date_to: str) -> list | None:
        payload = {
            "parameters": {},
        }
        # TODO: date validation
        if date_from:
            payload["parameters"]["date_from"] = date_from
        if date_to:
            payload["parameters"]["date_to"] = date_to
        return self._call_api(ds, payload)

    def campaign_reports(self, ds: Dataset, campaign_id: str, date_from: str, date_to: str) -> list | None:
        payload = {
            "parameters": {
                "ID_message": campaign_id,
            },
        }
        # TODO: date validation
        if date_from:
            payload["parameters"]["date_from"] = date_from
        if date_to:
            payload["parameters"]["date_to"] = date_to
        return self._call_api(ds, payload)

    def campaing_list(self, ds: Dataset, campaign_id: str) -> list | None:
        payload = {}
        if campaign_id:
            payload["ID_message"] = campaign_id
        return self._call_api(ds, payload)
