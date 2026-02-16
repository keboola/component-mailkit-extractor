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
    paging_key: str = ""
    depends_on: list[str] = field(default_factory=list)
    extra_primary_keys: list[str] = field(default_factory=list)

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

    CAMPAIGNS = Dataset("CAMPAIGNS", "mailkit.campaigns.list", "list of campaigns", "campaigns.csv", "ID_MESSAGE")
    REPORT = Dataset("REPORT", "mailkit.report", "summary report", "summaryreport.csv", "ID_MESSAGE")
    REPORT_CAMPAIGN = Dataset(
        "REPORT_CAMPAIGN",
        "mailkit.report.campaign",
        "campaign reports",
        "campaignreports.csv",
        "ID_SEND",
        depends_on=[str(REPORT)],  # ID_MESSAGE is mandatory, we use values loaded in REPORT (if not defined by user)
    )
    MSG_LINKS = Dataset(
        "MSG_LINKS",
        "mailkit.report.message.links",
        "message links",
        "links.csv",
        "ID_URL",
        depends_on=[str(REPORT_CAMPAIGN)],  # ID_SEND is mandatory, we use values loaded in REPORT_CAMPAIGN
    )
    RAW_MESSAGES = Dataset(
        "RAW_MESSAGES", "mailkit.report.raw.messages", "raw messages", "raw_messages.csv", "ID_send_message"
    )
    RAW_BOUNCES = Dataset(
        "RAW_BOUNCES",
        "mailkit.report.raw.bounces",
        "raw bounces",
        "raw_bounces.csv",
        primary_key="ID_UNDELIVERED_LOG",  # holy cow!
        paging_key="ID_undelivered_log",  # https://www.mailkit.com/cz/podpora/api/statistiky/mailkitreportrawbounces
    )
    RAW_RESPONSES = Dataset(
        "RAW_RESPONSES", "mailkit.report.raw.responses", "raw responses", "raw_responses.csv", "ID_log"
    )
    MLIST_UNSUBSCRIBED = Dataset(
        "MLIST_UNSUBSCRIBED", "mailkit.mailinglist.unsubscribed", "unsubscribed emails", "unsubscribed.csv", "EMAIL"
    )
    ENGAGEMENT = Dataset(
        "ENGAGEMENT",
        "mailkit.mailinglist.engagement",
        "engagement scores",
        "engagement.csv",
        "ID_EMAIL",
        paging_key="ID_email",
        extra_primary_keys=["ID_USER_LIST"],
    )

    # The following enum values are not implemented in the current version as they were not used by the clients at all.
    # We keep them here just for backwards compatibility of the configurations.
    ALL = Dataset("ALL", "N/A")
    REPORT_MSG = Dataset("REPORT_MSG", "mailkit.report.message", depends_on=[str(REPORT_CAMPAIGN)])
    MSG_RECIPIENTS = Dataset("MSG_RECIPIENTS", "mailkit.report.message.recipients", depends_on=[str(REPORT_CAMPAIGN)])
    MSG_FEEDBACK = Dataset("MSG_FEEDBACK", "mailkit.report.message.feedback", depends_on=[str(REPORT_CAMPAIGN)])
    LINKS_VISITORS = Dataset(
        "LINKS_VISITORS", "mailkit.report.message.links.visitors", depends_on=[str(REPORT_CAMPAIGN)]
    )  # this dataset has one more dependency (ID_URL)
    MSG_BOUNCES = Dataset("MSG_BOUNCES", "mailkit.report.message.bounces", depends_on=[str(REPORT_CAMPAIGN)])


class DateRangeEnum(str, Enum):
    RELATIVE = "relative"
    ABSOLUTE = "absolute"


class Configuration(BaseModel):
    client_id: str = Field(alias="clientId")
    client_md5: str = Field(alias="#clientMd5")

    datasets: list[DatasetsEnum] = Field(default_factory=list)

    date_range: DateRangeEnum = Field(alias="dateRange", default=DateRangeEnum.RELATIVE)

    days_period: int | None = Field(alias="daysPeriod", default=7)
    date_from: str | None = Field(alias="dateFrom", default="")  # TODO: Pydantic validation
    date_to: str | None = Field(alias="dateTo", default="")  # TODO: Pydantic validation

    campaign_ids: list[str] = Field(alias="campaignIds", default_factory=list)
    mailing_list_ids: list[str] = Field(alias="mailingListIds", default_factory=list)

    @computed_field
    @cached_property
    def date_range_to(self) -> str:
        if self.date_range == DateRangeEnum.RELATIVE and self.days_period:
            return date.today().isoformat()
        if self.date_range == DateRangeEnum.ABSOLUTE:
            return self.date_to or ""
        return ""

    @computed_field
    @cached_property
    def date_range_from(self) -> str:
        if self.date_range == DateRangeEnum.RELATIVE and self.days_period:
            date_from = date.today() - timedelta(days=self.days_period)
            return date_from.isoformat()
        if self.date_range == DateRangeEnum.ABSOLUTE:
            return self.date_from or ""
        return ""
