# Component Mailkit Extractor

This component extracts data from Mailkit API and stores it in Keboola.

## Configuration Parameters

- `clientId` (string): Client ID as stated in Mailkit (Profil – Integrace – API ID)
- `#clientMd5` (string): Client MD5 as stated in Mailkit (Profil – Integrace – MD5 kód)
- `datasets` (array): List of datasets to extract. Available options:
  - `CAMPAIGNS`: List of campaigns
  - `REPORT`: Summary report
  - `REPORT_CAMPAIGN`: Campaign reports
  - `MSG_LINKS`: Message links
  - `RAW_MESSAGES`: Raw messages
  - `RAW_BOUNCES`: Raw bounces
  - `RAW_RESPONSES`: Raw responses
  - `MLIST_UNSUBSCRIBED`: Unsubscribed emails
- `dateRange` (string): Type of date range filter. Options: `relative` or `absolute`
- `daysPeriod` (integer): Number of days to fetch (for relative date range). Default: 7
- `dateFrom` (string): Start date in ISO format (for absolute date range)
- `dateTo` (string): End date in ISO format (for absolute date range)
- `campaignIds` (array): Optional list of specific campaign IDs to extract

### Example Configuration

```json
{
  "clientId": "your-client-id",
  "#clientMd5": "your-md5-hash",
  "datasets": ["CAMPAIGNS", "REPORT", "RAW_BOUNCES"],
  "dateRange": "relative",
  "daysPeriod": 7
}
```
