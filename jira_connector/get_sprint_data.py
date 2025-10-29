from numpy import add
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv
import json
import base64
from typing import Optional
from datetime import date


def _to_dt(val) -> Optional[date]:
    """Parse various date/time representations and return a date (no time).

    Returns None when parsing fails or value is null.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        # pandas can handle common JIRA ISO8601 formats and naive strings
        dt = pd.to_datetime(val, utc=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


VALID_ISSUE_TYPES = ["Story", "Bug", "Defect", "Production Support"]
RELEVANT_FIELD_MAP = [
    "ID",
    "Link",
    "Title",
    "Backlog",
    "Current_Status_Category",
    "Item_Rank",
    "Updated",
    "Issue_Type",
    "Priority",
    "Fix_versions",
    "Components",
    "Assignee",
    "Reporter",
    "Project",
    "Resolution",
    "Labels",
    "Blocked_Days",
    "Blocked",
    "Parent",
    "done_datetime",
    "Sprints_JSON",
    "Story_Points",
    "Change_Log",
]


def get_sprint_in_board():
    load_dotenv()
    JIRA_URL = os.getenv("JIRA_URL")
    JIRA_EMAIL = os.getenv("USER_ID")
    JIRA_API_KEY = os.getenv("API_KEY")
    auth = f"{JIRA_EMAIL}:{JIRA_API_KEY}"
    encoded_auth = base64.b64encode(auth.encode()).decode()

    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {encoded_auth}",
    }
    url = f"{JIRA_URL}/rest/agile/1.0/board/371/sprint"
    response_DAP = requests.request("GET", url, headers=headers, verify=False)

    url = f"{JIRA_URL}/rest/agile/1.0/board/2770/sprint"
    response_DGTL = requests.request("GET", url, headers=headers, verify=False)

    sprint_dict = {}
    for sprint_info in response_DGTL.json().get("values", []) + response_DAP.json().get(
        "values", []
    ):
        sprint_id = str(sprint_info["id"])
        sprint_name = sprint_info["name"]
        sprint_dict[sprint_id] = {
            "sprint_name": sprint_name,
            # Normalize to timezone-aware timestamps for reliable comparisons
            "start_date": _to_dt(sprint_info.get("startDate")),
            "end_date": _to_dt(sprint_info.get("endDate")),
        }
    return sprint_dict


def get():
    df = pd.read_csv("data/jira_issues_historical.csv")
    # Filter the DataFrame to include only valid issue types
    filtered_df = df[df["Issue_Type"].isin(VALID_ISSUE_TYPES)]
    filtered_df = filtered_df[RELEVANT_FIELD_MAP]
    filtered_df = filtered_df.drop_duplicates()

    sprint_dict = get_sprint_in_board()

    for sprint in filtered_df["Sprints_JSON"].dropna().unique():
        sprint_data = json.loads(sprint)
        for sprint_info in sprint_data:
            sprint_id = str(sprint_info["id"])
            sprint_name = sprint_info["name"]
            sprint_dict[sprint_id] = {
                "sprint_name": sprint_name,
                # Normalize here as well; embedded JSON may differ from API shape
                "start_date": _to_dt(sprint_info.get("startDate")),
                "end_date": _to_dt(sprint_info.get("endDate")),
            }

    def extract_sprint_ids(sprint_dict, sprints_json, change_log_json):
        sprint_ids = set()
        # print("change_log_json:", change_log_json)
        if pd.notna(sprints_json):
            sprint_data = json.loads(sprints_json)
            sprint_ids.update([str(sprint_info["id"]) for sprint_info in sprint_data])
        if pd.notna(change_log_json):
            change_log_data = json.loads(change_log_json)
            for sprint_info in change_log_data["histories"]:
                if "items" in sprint_info:
                    for item in sprint_info["items"]:
                        if (
                            item["fieldtype"] == "custom"
                            and item["fieldId"] == "customfield_10005"
                        ):
                            if item.get("to"):
                                for sprint_id in item["to"].split(", "):
                                    try:
                                        if (
                                            sprint_dict[sprint_id]["start_date"]
                                            <= _to_dt(sprint_info["created"])
                                            <= sprint_dict[sprint_id]["end_date"]
                                        ):
                                            sprint_ids.add(str(sprint_id))
                                    except KeyError:
                                        continue
                            if item.get("from"):
                                for sprint_id in item["from"].split(", "):
                                    try:
                                        if (
                                            sprint_dict[sprint_id]["start_date"]
                                            < _to_dt(sprint_info["created"])
                                            <= sprint_dict[sprint_id]["end_date"]
                                        ):
                                            sprint_ids.add(str(sprint_id))
                                    except KeyError:
                                        continue
        return sprint_ids

    filtered_df["Sprints"] = filtered_df.apply(
        # TODO: Add change log to this
        lambda x: ", ".join(
            extract_sprint_ids(sprint_dict, x["Sprints_JSON"], x["Change_Log"])
            if pd.notnull(x["Sprints_JSON"]) and pd.notnull(x["Change_Log"])
            else []
        ),
        axis=1,
    )
    filtered_df.to_csv("data/with_sprints.csv", index=False)
    filtered_df["Sprints"] = filtered_df["Sprints"].str.split(r"\s*,\s*")
    filtered_df = filtered_df.explode("Sprints", ignore_index=True)
    filtered_df["Sprints"] = filtered_df["Sprints"].str.strip()
    filtered_df = filtered_df.drop_duplicates()

    issue_by_sprints = filtered_df.groupby("Sprints")
    by_ticket_types = []
    for sprint, group in issue_by_sprints:
        try:
            if sprint == "":
                continue
            sprint_data = {"Sprint": sprint_dict[sprint]["sprint_name"]}
            sprint_data.update(group["Issue_Type"].value_counts().to_dict())
            by_ticket_types.append(sprint_data)
        except KeyError:
            pass
    pd.DataFrame(by_ticket_types).to_csv("data/by_ticket_types.csv", index=False)

    by_ticket_counts = []
    for sprint, group in issue_by_sprints:
        try:
            if sprint == "":
                continue
            sprint_data = {
                "Sprint": sprint_dict[sprint]["sprint_name"],
                "Initial": 0,
                "Added": 0,
                "Removed": 0,
                "Blocked": 0,
                "Initial_Points": 0,
                "Added_Points": 0,
                "Removed_Points": 0,
                "Blocked_Points": 0,
            }
            initial_issues = set()
            added_issues = set()
            removed_issues = set()
            sprint_start_dt = sprint_dict[sprint].get("start_date")
            for issue in group.itertuples():
                if issue.Current_Status_Category == "Blocked":
                    sprint_data["Blocked"] += 1
                    sprint_data["Blocked_Points"] += (
                        issue.Story_Points if pd.notnull(issue.Story_Points) else 0
                    )
                change_log = json.loads(issue.Change_Log)

                decided_added_or_removed = False
                for history in change_log["histories"]:
                    for item in history["items"]:
                        if item["field"] == "Sprint":
                            sprint_added = set(item["toString"].split(", ")) - set(
                                item["fromString"].split(", ")
                            )
                            history_created_dt = _to_dt(history.get("created"))
                            if sprint_dict[sprint]["sprint_name"] in sprint_added:
                                if history_created_dt <= sprint_start_dt:
                                    if issue.ID not in added_issues:
                                        initial_issues.add(issue.ID)
                                else:
                                    if issue.ID not in initial_issues:
                                        added_issues.add(issue.ID)
                                decided_added_or_removed = True
                                break

                            sprint_removed = set(item["fromString"].split(", ")) - set(
                                item["toString"].split(", ")
                            )
                            if (
                                sprint_dict[sprint]["sprint_name"] in sprint_removed
                                and history_created_dt >= sprint_start_dt
                            ):
                                removed_issues.add(issue.ID)
                            decided_added_or_removed = True
                            break
                    if decided_added_or_removed:
                        break
                else:
                    if issue not in added_issues and issue not in initial_issues:
                        backlog_dt = _to_dt(issue.Backlog)
                        if (
                            backlog_dt is not None
                            and sprint_start_dt is not None
                            and backlog_dt < sprint_start_dt
                        ):
                            initial_issues.add(issue.ID)
                        else:
                            added_issues.add(issue.ID)
            sprint_data["Initial"] = len(initial_issues)
            sprint_data["Initial_Points"] = sum(
                issue.Story_Points
                for issue in group.itertuples()
                if issue.ID in initial_issues and pd.notnull(issue.Story_Points)
            )
            if sprint == "8246":
                print("init", initial_issues)
            # print(initial_issues)
            sprint_data["Added"] = len(added_issues)
            sprint_data["Added_Points"] = sum(
                issue.Story_Points
                for issue in group.itertuples()
                if issue.ID in added_issues and pd.notnull(issue.Story_Points)
            )
            if sprint == "8246":
                print("added", added_issues)
            sprint_data["Removed"] = len(removed_issues)
            sprint_data["Removed_Points"] = sum(
                issue.Story_Points
                for issue in group.itertuples()
                if issue.ID in removed_issues and pd.notnull(issue.Story_Points)
            )
            if sprint == "8246":
                print("rm", removed_issues)
            # print(removed_issues)
            by_ticket_counts.append(sprint_data)
        except KeyError:
            pass
    # print("By Ticket Counts:\n", by_ticket_counts)
    pd.DataFrame(by_ticket_counts).to_csv("data/by_ticket_counts.csv", index=False)


if __name__ == "__main__":
    # Sample data for demonstration
    get()
    # get_permit()
    # get_sprint_in_board()
