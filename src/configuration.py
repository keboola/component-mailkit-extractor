from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, Field


@dataclass
class Dataset:
    title: str
    api_function: str = ""
    description: str = ""
    filename: str = ""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dataset) and not isinstance(other, str):
            return NotImplemented
        if isinstance(other, str):
            return self.title == other
        if isinstance(other, Dataset):
            return self.title == other.title
        return False


class DatasetsEnum(Enum):
    # special case, doesn't have to have the arguments filled in as it will be used just to enable all other datasets
    ALL = Dataset("ALL")
    CAMPAIGNS = Dataset("CAMPAIGNS", "mailkit.campaigns.list", "list of campaigns", "campaigns.csv")
    REPORT = Dataset("REPORT", "mailkit.report", "summary report", "summaryreport.csv")
    REPORT_CAMPAIGN = Dataset("REPORT_CAMPAIGN", "mailkit.report.campaign", "campaign reports", "campaignreports.csv")
    REPORT_MSG = Dataset("REPORT_MSG")
    MSG_RECIPIENTS = Dataset("MSG_RECIPIENTS")
    MSG_FEEDBACK = Dataset("MSG_FEEDBACK")
    MSG_LINKS = Dataset("MSG_LINKS")
    LINKS_VISITORS = Dataset("LINKS_VISITORS")
    MSG_BOUNCES = Dataset("MSG_BOUNCES")
    RAW_BOUNCES = Dataset("RAW_BOUNCES")
    RAW_RESPONSES = Dataset("RAW_RESPONSES")
    RAW_MESSAGES = Dataset("RAW_MESSAGES")


class Configuration(BaseModel):
    client_id: str = Field(alias="clientId")
    client_md5: str = Field(alias="#clientMd5")

    datasets: list[DatasetsEnum] = Field(default_factory=list)

    since_last_run: bool = Field(alias="sinceLastRun", default=False)
    days_period: int | None = Field(alias="daysPeriod", default=0)
    date_from: str | None = Field(alias="dateFrom", default="")
    date_to: str | None = Field(alias="dateTo", default="")

    campaign_ids: list[str] = Field(alias="campaignIds", default_factory=list)
