import logging
from dataclasses import dataclass
from typing import Any

import requests
from keboola.component.exceptions import UserException

from configuration import MAILINGLIST_LIST_DS, Dataset

ENDPOINT = "https://api.mailkit.eu/json.fcgi"
BATCH_SIZE = 5_000


@dataclass
class PagingResult:
    items: list[dict]
    next_id: str


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
                logging.info("Getting %s: OK (%i items)", ds.description, len(result))
            return result
        except Exception as e:
            logging.exception("Failed to get %s: %s", ds.description, e)

        return None

    def campaigns_list(self, ds: Dataset, campaign_id: str = "") -> list | None:
        # https://www.mailkit.com/cz/podpora/api/sprava-kampani/mailkitcampaignslist
        payload = {}
        if campaign_id:
            payload["ID_message"] = campaign_id
        return self._call_api(ds, payload)

    def report(self, ds: Dataset, date_from: str, date_to: str) -> list | None:
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreport
        payload = {
            "parameters": {},
        }
        if date_from:
            payload["parameters"]["range_from"] = date_from
        if date_to:
            payload["parameters"]["range_to"] = date_to
        return self._call_api(ds, payload)

    def campaign_reports(
        self,
        ds: Dataset,
        campaign_id: str,
        date_from: str,
        date_to: str,
    ) -> list | None:
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreportcampaign
        payload = {
            "parameters": {
                "ID_message": campaign_id,
            },
        }
        if date_from:
            payload["parameters"]["range_from"] = date_from
        if date_to:
            payload["parameters"]["range_to"] = date_to
        return self._call_api(ds, payload)

    def message_links(self, ds: Dataset, id_send: str, campaign_id: str = "") -> list | None:
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreportmessagelinks
        payload = {
            "parameters": {
                "ID_send": id_send,
            },
        }
        if campaign_id:
            payload["parameters"]["ID_message"] = campaign_id

        return self._call_api(ds, payload)

    def raw_messages_bounces_responses(self, ds: Dataset, campaign_id: str = "", next_id: str = "") -> PagingResult:
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreportrawmessages
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreportrawbounces
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreportrawresponses
        payload: dict[str, Any] = {
            "parameters": {
                "limit": BATCH_SIZE,
            },
        }
        if campaign_id:
            payload["parameters"]["ID_message"] = campaign_id
        if next_id:
            # When filled in, the paging key is used, otherwise we fall back
            # to the primary key (see RAW_BOUNCES dataset definition in configuration.py)
            payload["parameters"][ds.paging_key or ds.primary_key] = next_id

        items = self._call_api(ds, payload)
        if items:
            next_id = items[-1].get(ds.primary_key)  # use last item's id as next id

        return PagingResult(items or [], next_id)

    def mailinglist_list(self) -> list | None:
        # https://www.mailkit.com/resources/api/mailing-list-management/mailkitmailinglistlist
        return self._call_api(MAILINGLIST_LIST_DS, {})

    def mailinglist_engagement(
        self, ds: Dataset, id_user_list: str, id_email: str = "", limit: int = 25_000
    ) -> PagingResult:
        # https://www.mailkit.com/resources/api/mailing-list-management/mailkitmailinglistengagement
        payload: dict[str, Any] = {
            "parameters": {
                "ID_user_list": id_user_list,
                "limit": limit,
            },
        }
        if id_email:
            payload["parameters"]["ID_email"] = id_email

        items = self._call_api(ds, payload)
        next_id = ""
        if items:
            next_id = str(items[-1].get(ds.primary_key, ""))

        return PagingResult(items or [], next_id)

    def mailinglist_unsubscribed(self, ds: Dataset, date_from: str) -> list | None:
        # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitmalinglistunsubscribed
        payload = {}
        if date_from:
            payload["parameters"] = {
                "range_from": date_from,
            }
        return self._call_api(ds, payload)
