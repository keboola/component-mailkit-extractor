import csv
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from keboola.component import sync_actions
from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.http_client import HttpClient

from configuration import Configuration, Dataset, DatasetsEnum
from mailkit_client import MailkitClient, PagingResult


@dataclass
class WriterCacheEntry:
    table: TableDefinition
    fieldnames: list[str]
    last_row_id: str


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.params = Configuration(**self.configuration.parameters)
        self.mkc = MailkitClient(self.params.client_id, self.params.client_md5)
        self.storage_client = (
            HttpClient(
                self.environment_variables.url + "/v2/storage/",
                auth_header={"X-StorageApi-Token": self.environment_variables.token},
            )
            if self.environment_variables.token and self.environment_variables.url
            else None
        )

        self.campaign_ids: list[str] = self.params.campaign_ids or []
        self.send_ids: set[str] = set()

        self.writer_cache = {}

        # State file support for paging - load last seen IDs from state
        # Structure: {last_seen_ids: {endpoint_title: {campaign_id: last_id}}, campaign_ids: [...]}
        state = self.get_state_file()
        self.last_seen_ids: dict[str, dict[str, str]] = state.get("last_seen_ids", {})
        self._validate_campaign_ids_unchanged(state)

    def _validate_campaign_ids_unchanged(self, state: dict) -> None:
        """Check if campaign_ids changed since last run. Raise UserException if so."""
        if not self.last_seen_ids:
            return  # No existing state, nothing to validate
        stored_campaign_ids = state.get("campaign_ids")
        if stored_campaign_ids is None:
            return  # Old state format without campaign_ids tracking
        current_campaign_ids = sorted(self.params.campaign_ids or [])
        if stored_campaign_ids != current_campaign_ids:
            raise UserException(
                "Campaign filter has changed since the last run. "
                "To continue with the new filter, please clear the component state first. "
                "You can do this in the component configuration by clicking 'Reset State'."
            )

    def run(self):
        date_from = self.params.date_range_from
        date_to = self.params.date_range_to

        if date_from:
            logging.info("Date range start: %s", date_from)
        if date_to:
            logging.info("Date range end: %s", date_to)
        if not date_from and not date_to:
            logging.info("No date range specified, fetching all data.")

        # ensure all dependencies are included
        added_datasets = []
        for dataset in self.params.datasets:
            for dep in dataset.value.depends_on:
                if dep not in self.params.datasets:
                    added_datasets.append(dep)
                    logging.warning(
                        "Adding dataset %s as a dependency of the selected %s dataset.",
                        dep,
                        dataset.value.title,
                    )
        self.params.datasets.extend(added_datasets)

        for dataset in DatasetsEnum:
            if dataset not in self.params.datasets:
                continue

            ds = dataset.value

            data = None
            write_at_once = True
            logging.info(f"Processing dataset: {ds.title}")
            match dataset:
                case DatasetsEnum.CAMPAIGNS:
                    data = self._get_campaigns(ds)
                case DatasetsEnum.REPORT:
                    data = self._get_summary_report(ds, date_from, date_to)
                case DatasetsEnum.REPORT_CAMPAIGN:
                    data = self._get_campaign_reports(ds, date_from, date_to)
                case DatasetsEnum.MSG_LINKS:
                    data = self._get_message_links(ds)
                case DatasetsEnum.RAW_MESSAGES | DatasetsEnum.RAW_BOUNCES | DatasetsEnum.RAW_RESPONSES:
                    self._get_raw_items(ds)
                    write_at_once = False
                case DatasetsEnum.MLIST_UNSUBSCRIBED:
                    data = self._get_mailinglist_unsubscribed(ds, date_from)
                case DatasetsEnum.ENGAGEMENT:
                    self._get_engagement(ds)
                    write_at_once = False
                case _:
                    logging.warning(
                        "The %s dataset (API method: %s) is not implemented. If you believe this is an error, "
                        "please contact Keboola support. 🐙",
                        ds.title,
                        ds.api_function,
                    )
            if write_at_once:
                self._write_results(ds, data)

        # Write state file with last seen IDs for paging endpoints
        if self.last_seen_ids:
            self.write_state_file(
                {
                    "last_seen_ids": self.last_seen_ids,
                    "campaign_ids": sorted(self.params.campaign_ids or []),
                }
            )
            logging.info("State file saved: %s", self.last_seen_ids)

    def _get_existing_columns(self, filename: str) -> list[str]:
        if not self.storage_client:
            logging.info("Storage API not available, skipping existing columns check for %s", filename)
            return []
        component_id = self.environment_variables.component_id.replace(".", "-")
        config_id = self.environment_variables.config_id
        table_id = f"in.c-{component_id}-{config_id}.{Path(filename).stem}"
        logging.info("Fetching existing columns for table: %s", table_id)
        try:
            response = self.storage_client.get_raw(f"tables/{table_id}")
            if response.status_code == 404:
                logging.info("Table %s does not exist yet", table_id)
                return []
            if not response.ok:
                logging.warning("Failed to fetch columns for %s: HTTP %s", table_id, response.status_code)
                return []
            columns = response.json().get("columns", [])
            logging.info("Found existing columns for %s: %s", table_id, columns)
            return columns
        except Exception as e:
            logging.warning("Could not fetch existing columns for %s: %s", table_id, e)
            return []

    @staticmethod
    def _get_fieldnames(data: list[dict], primary_key: str, expected_fields: list[str] | None = None) -> list[str]:
        fieldnames = set(expected_fields or [])
        for row in data:
            if primary_key not in row:
                raise Exception(
                    f"Primary key {primary_key} not found in data fields (keys: {', '.join(sorted(row.keys()))})."
                )
            fieldnames.update(row.keys())
        fieldnames.remove(primary_key)
        return [primary_key] + sorted(list(fieldnames))

    def _write_results(self, ds: Dataset, data: list[dict] | None, primary_key: list[str] | None = None) -> None:
        if not data:
            logging.warning("No data in the %s dataset", ds.title)
            return

        if ds.filename not in self.writer_cache:
            logging.info("Writing %s items to %s", len(data), ds.filename)
            pk = primary_key or [ds.primary_key]
            table = self.create_out_table_definition(ds.filename, incremental=True, primary_key=pk)
            existing_columns = self._get_existing_columns(ds.filename)
            fieldnames = self._get_fieldnames(data, ds.primary_key, existing_columns)

            with open(table.full_path, mode="w", encoding="utf-8", newline="") as out_file:
                cached_writer = csv.DictWriter(out_file, fieldnames=fieldnames, restval="")
                cached_writer.writeheader()
                self.writer_cache[ds.filename] = WriterCacheEntry(table, fieldnames, "")

            self.write_manifest(table)

        wc_entry = self.writer_cache[ds.filename]  # :-)
        table = wc_entry.table
        fieldnames = wc_entry.fieldnames
        last_row_id = wc_entry.last_row_id

        with open(table.full_path, mode="a", encoding="utf-8", newline="") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=fieldnames, restval="")
            for row in data:
                if row[ds.primary_key] == last_row_id:
                    continue
                writer.writerow(row)
                wc_entry.last_row_id = row[ds.primary_key]

    def _get_campaigns(self, ds: Dataset) -> list[dict]:
        campaigns = []

        campaign_ids = self.params.campaign_ids or [""]
        for campaign_id in campaign_ids:
            if data := self.mkc.campaigns_list(ds, campaign_id):
                campaigns.extend(data)

        return campaigns

    def _get_summary_report(self, ds: Dataset, date_from: str, date_to: str) -> list[dict]:
        """Get summary report and – if not filled in by user – populate the list of campaign IDs
        to be used for getting other datasets."""
        data = self.mkc.report(ds, date_from, date_to)

        if not data:
            return []

        if not self.campaign_ids:
            self.campaign_ids = list({c["ID_MESSAGE"] for c in data})
        logging.info("Parsing %s: %s unique campaigns", ds.description, len(self.campaign_ids))

        return data

    def _get_campaign_reports(self, ds: Dataset, date_from: str, date_to: str) -> list[dict]:
        """Gets campaign reports for all campaign IDs in the configuration."""
        campaigns = []

        for campaign_id in self.campaign_ids:
            if data := self.mkc.campaign_reports(ds, campaign_id, date_from, date_to):
                campaigns.extend(data)

        if not campaigns:
            return []

        self.send_ids |= {c["ID_SEND"] for c in campaigns}
        logging.info("Parsing %s: %s unique sends", ds.description, len(self.send_ids))

        return campaigns

    def _get_message_links(self, ds: Dataset) -> list[dict]:
        links = []

        campaign_ids = self.campaign_ids or [""]
        for campaign_id in campaign_ids:
            for send_id in self.send_ids:
                if data := self.mkc.message_links(ds, send_id, campaign_id):
                    links.extend(data)

        return links

    def _get_raw_items(self, ds: Dataset) -> None:
        campaign_ids = self.campaign_ids or [""]
        for campaign_id in campaign_ids:
            # Get initial next_id from state file for this specific campaign
            endpoint_state = self.last_seen_ids.get(ds.title, {})
            initial_next_id = endpoint_state.get(campaign_id, "")
            if initial_next_id:
                logging.info(
                    "Resuming %s dataset for campaign %s from ID: %s",
                    ds.title,
                    campaign_id or "(all)",
                    initial_next_id,
                )
            self._get_raw_items_by_campaign(ds, campaign_id, initial_next_id)

    def _paginate(
        self,
        ds: Dataset,
        fetch_page: Callable[[str], PagingResult],
        on_page: Callable[[list[dict], str], None],
        min_page_size: int,
        initial_next_id: str = "",
    ) -> None:
        next_id = initial_next_id
        while True:
            paging_response = fetch_page(next_id)
            if not (data := paging_response.items):
                break
            on_page(data, paging_response.next_id)
            if min_page_size and len(data) < min_page_size:
                logging.info(
                    "Page size %i below threshold %i, stopping %s (%s)",
                    len(data),
                    min_page_size,
                    ds.title,
                    ds.description,
                )
                paging_response.items.clear()
                break
            paging_response.items.clear()
            if not paging_response.next_id or paging_response.next_id == next_id:
                break
            logging.info(
                "Fetching next page of %s dataset, starting from ID %s",
                ds.title,
                paging_response.next_id,
            )
            next_id = paging_response.next_id

    def _get_raw_items_by_campaign(self, ds: Dataset, campaign_id: str = "", next_id: str = "") -> None:
        def on_page(data: list[dict], page_next_id: str) -> None:
            self._write_results(ds, data)
            if ds.title not in self.last_seen_ids:
                self.last_seen_ids[ds.title] = {}
            self.last_seen_ids[ds.title][campaign_id] = page_next_id

        self._paginate(
            ds,
            fetch_page=lambda nid: self.mkc.raw_messages_bounces_responses(ds, campaign_id, nid),
            on_page=on_page,
            min_page_size=self.params.min_page_size,
            initial_next_id=next_id,
        )

    def _get_engagement(self, ds: Dataset) -> None:
        mailing_list_ids = self.params.mailing_list_ids
        if not mailing_list_ids:
            logging.info("No Mailing List IDs configured, fetching all mailing lists automatically.")
            lists = self.mkc.mailinglist_list()
            if not lists:
                raise UserException(
                    "Failed to fetch mailing lists from Mailkit. "
                    "Please verify your credentials or specify Mailing List IDs manually."
                )
            mailing_list_ids = [ml["ID_USER_LIST"] for ml in lists if ml.get("STATUS") == "enabled"]
            if not mailing_list_ids:
                raise UserException("No enabled mailing lists found in your Mailkit account.")
            logging.info("Auto-detected %s enabled mailing list(s): %s", len(mailing_list_ids), mailing_list_ids)

        for list_id in mailing_list_ids:
            logging.info("Fetching engagement scores for mailing list %s", list_id)
            if ds.filename in self.writer_cache:
                self.writer_cache[ds.filename].last_row_id = ""

            def on_page(data: list[dict], _next_id: str, list_id=list_id) -> None:
                for row in data:
                    row["ID_USER_LIST"] = list_id
                self._write_results(ds, data, primary_key=["ID_EMAIL", "ID_USER_LIST"])

            self._paginate(
                ds,
                fetch_page=lambda nid, lid=list_id: self.mkc.mailinglist_engagement(ds, lid, id_email=nid),
                on_page=on_page,
                min_page_size=self.params.min_page_size,
            )

    def _get_mailinglist_unsubscribed(self, ds: Dataset, date_from: str) -> list[dict]:
        if data := self.mkc.mailinglist_unsubscribed(ds, date_from):
            return data
        return []

    @sync_action("verifyCredentials")
    def verify_credentials(self):
        if self.mkc.campaigns_list(DatasetsEnum.CAMPAIGNS.value):
            return sync_actions.ValidationResult("Verification successful", sync_actions.MessageType.SUCCESS)
        return sync_actions.ValidationResult("Failed to verify credentials", sync_actions.MessageType.ERROR)


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
