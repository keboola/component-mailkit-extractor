from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from functools import cached_property

from pydantic import BaseModel, Field, computed_field


@dataclass
class Dataset:
    title: str
    api_function: str = ""
    description: str = ""
    filename: str = ""
    primary_key: str = ""
    depends_on: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return self.title

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Dataset) and not isinstance(other, str):
            return NotImplemented
        if isinstance(other, str):
            return str(self) == other
        if isinstance(other, Dataset):
            return str(self) == str(other)
        return False


class DatasetsEnum(Enum):
    """
    The order of the datasets here matters, as certain reports depend on previous ones.
    """

    # special case, doesn't have to have the arguments filled in as it will be used just to enable all other datasets
    # TODO: remove 🤔
    ALL = Dataset("ALL")

    CAMPAIGNS = Dataset("CAMPAIGNS", "mailkit.campaigns.list", "list of campaigns", "campaigns.csv", "ID_MESSAGE")
    REPORT = Dataset("REPORT", "mailkit.report", "summary report", "summaryreport.csv", "ID_MESSAGE")
    REPORT_CAMPAIGN = Dataset(
        "REPORT_CAMPAIGN", "mailkit.report.campaign", "campaign reports", "campaignreports.csv", "ID_SEND"
    )

    # not implemented
    REPORT_MSG = Dataset("REPORT_MSG", "mailkit.report.message", depends_on=[str(REPORT_CAMPAIGN)])
    MSG_RECIPIENTS = Dataset("MSG_RECIPIENTS", "mailkit.report.message.recipients", depends_on=[str(REPORT_CAMPAIGN)])
    MSG_FEEDBACK = Dataset("MSG_FEEDBACK", "mailkit.report.message.feedback", depends_on=[str(REPORT_CAMPAIGN)])

    MSG_LINKS = Dataset(
        "MSG_LINKS",
        "mailkit.report.message.links",
        "message links",
        "links.csv",
        "ID_URL",
        depends_on=[str(REPORT_CAMPAIGN)],
    )

    # not implemented
    LINKS_VISITORS = Dataset(
        "LINKS_VISITORS", "mailkit.report.message.links.visitors", depends_on=[str(REPORT_CAMPAIGN)]
    )  # one more dependency (ID_URL)
    MSG_BOUNCES = Dataset("MSG_BOUNCES", "mailkit.report.message.bounces", depends_on=[str(REPORT_CAMPAIGN)])

    RAW_MESSAGES = Dataset(
        "RAW_MESSAGES", "mailkit.report.raw.messages", "raw messages", "raw_messages.csv", "ID_send_message"
    )
    # TODO: TYPO in Mailkit API 🤯 -------------------------------------------------------------> 👇
    RAW_BOUNCES = Dataset(
        "RAW_BOUNCES", "mailkit.report.raw.bounces", "raw bounces", "raw_bounces.csv", "ID_SEND_MESSGE"
    )
    RAW_RESPONSES = Dataset(
        "RAW_RESPONSES", "mailkit.report.raw.responses", "raw responses", "raw_responses.csv", "ID_send_message"
    )


class Configuration(BaseModel):
    client_id: str = Field(alias="clientId")
    client_md5: str = Field(alias="#clientMd5")

    datasets: list[DatasetsEnum] = Field(default_factory=list)

    days_period: int | None = Field(alias="daysPeriod", default=0)
    date_from: str | None = Field(alias="dateFrom", default="")
    date_to: str | None = Field(alias="dateTo", default="")

    campaign_ids: list[str] = Field(alias="campaignIds", default_factory=list)

    # obsolete, deprecated parameter, never implemented even in the previous versions
    # keeping it here just for configuration compatibility
    since_last_run: bool = Field(alias="sinceLastRun")

    @computed_field
    @cached_property
    def date_range_to(self) -> str:
        if self.days_period:
            return date.today().isoformat()
        return self.date_to or ""

    @computed_field
    @cached_property
    def date_range_from(self) -> str:
        if self.days_period:
            date_from = date.today() - timedelta(days=self.days_period)
            return date_from.isoformat()
        return self.date_from or ""
