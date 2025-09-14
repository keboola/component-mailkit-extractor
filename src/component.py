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

    def run(self):
        start_date = self.params.date_from or ""
        end_date = self.params.date_to or ""

        get_all = DatasetsEnum.ALL in self.params.datasets
        for dataset in DatasetsEnum:
            if dataset == DatasetsEnum.ALL:
                continue
            if not get_all and dataset not in self.params.datasets:
                continue

            ds = dataset.value

            data = None
            logging.info(f"Processing dataset: {ds.title}")
            match dataset:
                case DatasetsEnum.REPORT:
                    data = self._get_report(ds, start_date, end_date)
                # case EndpointsEnum.REPORT_CAMPAIGN:
                #     data = self._get_campaign_reports(start_date, end_date)
                #     filename = "campaignreports.csv"
                case DatasetsEnum.CAMPAIGNS:
                    data = self._get_campaigns(ds)
                case _:
                    logging.warning(f"Dataset {ds.title} is not implemented yet.")

            if data is not None:
                self._write_results(ds.filename, data, "ID_MESSAGE")

    def _get_fieldnames(self, data: list[dict], primary_key: str) -> list[str]:
        fieldnames = set()
        for row in data:
            if primary_key not in row:
                raise Exception(f"Primary key {primary_key} not found in data fields.")
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

    def _get_report(self, ds: Dataset, date_from: str, date_to: str):
        campaigns = []

        if data := self.mkc.summary_report(ds, date_from, date_to):
            logging.info("%i campaigns", len(data))
            campaigns.extend(data)

        return campaigns

    def _get_campaign_reports(self, ds: Dataset, date_from: str, date_to: str):
        campaigns = []

        if data := self.mkc.campaign_reports(ds, "", date_from, date_to):
            logging.info("%i campaigns", len(data))
            campaigns.extend(data)

        return campaigns

    def _get_campaigns(self, ds: Dataset):
        campaigns = []

        campaign_ids = self.params.campaign_ids or [""]
        for campaign_id in campaign_ids:
            if data := self.mkc.campaing_list(ds, campaign_id):
                logging.info("%i campaigns", len(data))
                campaigns.extend(data)

        return campaigns

    @sync_action("verifyCredentials")
    def verify_credentials(self):
        if self.mkc.campaing_list(""):
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
