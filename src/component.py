import csv
import logging

from keboola.component import sync_actions
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException

from configuration import Configuration, Dataset, DatasetsEnum
from mailkit_client import MailkitClient


class Component(ComponentBase):
    def __init__(self):
        super().__init__()

        self.params = Configuration(**self.configuration.parameters)
        self.mkc = MailkitClient(self.params.client_id, self.params.client_md5)
        self.campaign_ids: list[str] = self.params.campaign_ids or []
        self.send_ids: set[str] = set()

    def run(self):
        date_from = self.params.date_range_from
        date_to = self.params.date_range_to

        if date_from:
            logging.info("Date range start: %s", date_from)
        if date_to:
            logging.info("Date range end: %s", date_to)
        if not date_from and not date_to:
            logging.info("No date range specified, fetching all data.")

        if get_all := DatasetsEnum.ALL in self.params.datasets:
            logging.info("Processing all datasets")
        else:
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
            if dataset == DatasetsEnum.ALL:
                continue
            if not get_all and dataset not in self.params.datasets:
                continue

            ds = dataset.value

            data = None
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
                    data = self._get_raw_messages_bounces_responses(ds)
                case _:
                    logging.warning(f"The {ds.title} dataset ({ds.api_function}) is not implemented.")

            if data is not None:
                self._write_results(ds.filename, data, ds.primary_key)

    def _get_fieldnames(self, data: list[dict], primary_key: str) -> list[str]:
        fieldnames = set()
        for row in data:
            if primary_key not in row:
                raise Exception(
                    f"Primary key {primary_key} not found in data fields (keys: {', '.join(sorted(row.keys()))})."
                )
            fieldnames.update(row.keys())
        fieldnames.remove(primary_key)
        return [primary_key] + sorted(list(fieldnames))

    def _write_results(self, filename: str, data: list[dict], primary_key: str):
        table = self.create_out_table_definition(filename, incremental=True, primary_key=[primary_key])

        with open(table.full_path, mode="w", encoding="utf-8", newline="") as out_file:
            if not data:
                logging.warning("No data to write to the output file.")
                return

            fieldnames = self._get_fieldnames(data, primary_key)
            writer = csv.DictWriter(out_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

        self.write_manifest(table)

    def _get_campaigns(self, ds: Dataset) -> list[dict]:
        campaigns = []

        campaign_ids = self.params.campaign_ids or [""]
        for campaign_id in campaign_ids:
            if data := self.mkc.campaing_list(ds, campaign_id):
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

        for send_id in self.send_ids:
            if data := self.mkc.message_links(ds, send_id):
                links.extend(data)

        return links

    def _get_raw_messages_bounces_responses(self, ds: Dataset) -> list[dict]:
        if data := self.mkc.raw_messages_bounces_responses(ds):
            return data
        return []

    @sync_action("verifyCredentials")
    def verify_credentials(self):
        if self.mkc.campaing_list(DatasetsEnum.CAMPAIGNS.value, ""):
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
