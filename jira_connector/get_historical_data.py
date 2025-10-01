"""
This module handles the retrieval and processing of historical Jira issue data.

It provides functionality to connect to Jira, fetch issues from specified projects,
and transform the data into a structured format with consistent field mappings.
The module supports various issue fields including ID, status, priority, versions,
and custom fields, making it suitable for historical data analysis and reporting.

Key features:
- Connects to Jira using environment credentials
- Retrieves issues from predefined project keys
- Maps Jira fields to standardized column names
- Handles date parsing and field transformations
- Supports custom field mappings for specific data requirements
"""

import os
import warnings
from datetime import date, datetime, timezone
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta
from jira import JIRA
import pandas as pd

# Constants
RELEVANT_PROJECT_KEYS = [
    "PS2",
    "ITE",
    "PLE",
    "ITP",
    "ORT",
    "CUS",
    "PER",
    "MOB",
    "ORC",
    "OSI",
    "PRD",
]

HISTORICAL_FIELD_MAP = {
    "ID": lambda issue: issue["key"],
    "Link": lambda issue: f"https://abcsupply.atlassian.net/browse/{issue['key']}",
    "Title": lambda issue: issue["fields"]["summary"],
    "Backlog": lambda issue: parse_date(issue["fields"]["created"]),
    "Current_Status_Category": lambda issue: issue["fields"]["status"]["name"],
    "Item_Rank": lambda issue: issue["fields"]["customfield_10000"],
    "Updated": lambda issue: parse_date(issue["fields"]["updated"]),
    "Issue_Type": lambda issue: issue["fields"]["issuetype"]["name"],
    "Priority": lambda issue: issue["fields"]["priority"]["name"],
    "Fix_versions": lambda issue: ", ".join(
        [v["name"] for v in issue["fields"]["fixVersions"]]
    ),
    "Components": lambda issue: ", ".join(
        [c["name"] for c in issue["fields"]["components"]]
    ),
    "Assignee": lambda issue: issue["fields"]["assignee"]["displayName"],
    "Reporter": lambda issue: issue["fields"]["reporter"]["displayName"],
    "Project": lambda issue: issue["fields"]["project"]["key"],
    "Resolution": lambda issue: issue["fields"]["resolution"]["name"],
    "Labels": lambda issue: (
        f"[{"|".join(issue["fields"]["labels"])}]" if issue["fields"]["labels"] else ""
    ),
    "Blocked_Days": lambda issue: "",  # TODO: Implement blocked days calculation
    "Blocked": lambda issue: "FALSE",  # TODO: Implement blocked status
    "Parent": lambda issue: issue["fields"]["parent"]["key"],
    "done_datetime": lambda issue: datetime.strptime(issue["fields"]["resolutiondate"], "%Y-%m-%dT%H:%M:%S.%f%z").replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
,
}
# Example format: 2025-09-29T13:36:31.190-0500


def parse_date(date_string):
    """Parse date string from Jira API."""
    return datetime.strptime(date_string.split("T")[0], "%Y-%m-%d").date()


def setup_jira_connection():
    """Initialize and return JIRA connection."""
    load_dotenv()
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    jira_options = {"verify": False}
    return JIRA(
        options=jira_options,
        server=os.getenv("JIRA_URL"),
        basic_auth=(os.getenv("USER_ID"), os.getenv("API_KEY")),
    )


def fetch_jira_issues(jira, base_date, max_fetch=None):
    """Fetch issues from JIRA using pagination."""
    jql_query = (
        f'project in ({",".join(RELEVANT_PROJECT_KEYS)}) '
        f'AND updated >= "{base_date}" order by updated DESC'
    )

    all_issues = []
    chunk_size = 100
    next_page_token = None

    while True:
        issues = jira.enhanced_search_issues(
            jql_query,
            fields="*all",
            expand="changelog",
            nextPageToken=next_page_token,
            maxResults=chunk_size,
            json_result=True,
        )

        batch = issues["issues"]
        all_issues.extend(batch)
        print(f"Retrieved {len(batch)} issues. Total: {len(all_issues)}")

        if max_fetch and len(all_issues) >= max_fetch:
            print(f"Reached max fetch limit of {max_fetch}")
            break

        if issues["isLast"]:
            print("Reached end of results")
            break

        next_page_token = issues["nextPageToken"]

    return all_issues


def process_historical_data(issues):
    """Process historical data from issues."""
    data = []
    for issue in issues:
        row = {}
        for field in HISTORICAL_FIELD_MAP:
            field_func = HISTORICAL_FIELD_MAP[field]
            try:
                value = field_func(issue)
            except Exception:
                value = None
            row[field] = value
        data.append(row)

    return pd.DataFrame(data)


def process_status_changes(issues):
    """Process status change history from issues."""
    status_changes = []

    for issue in issues:
        for history in issue["changelog"]["histories"]:
            for item in history["items"]:
                if item["field"] != "status":
                    continue

                status_changes.append(
                    {
                        "ID": issue["key"],
                        "Status Change Date": parse_date(history["created"]),
                        "Status Change From": item["fromString"],
                        "Status Change To": item["toString"],
                    }
                )

    return pd.DataFrame(status_changes)


def get(max_fetch=None):
    """
    Main function to fetch and process JIRA data.

    Args:
        max_fetch: Limit number of tickets fetched

    Returns:
        pd.DataFrame: Processed and pivoted dataframe
    """
    jira = setup_jira_connection()
    print("Connected to Jira successfully!")

    base_date = date.today() - relativedelta(years=1, months=6)
    print(f"Fetching tickets updated since {base_date}...")

    issues = fetch_jira_issues(jira, base_date, max_fetch)

    historical_df = process_historical_data(issues)
    status_change_df = process_status_changes(issues)

    result_df = historical_df.merge(status_change_df, on="ID", how="left")
    print(f"Total unique issues: {len(result_df['ID'].unique())}")
    return result_df
