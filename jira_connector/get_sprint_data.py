import pandas as pd
from .get_historical_data import setup_jira_connection

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
]

if __name__ == "__main__":
    # Sample data for demonstration
    # df = pd.read_csv("data/jira_issues_historical.csv")
    # # Filter the DataFrame to include only valid issue types
    # filtered_df = df[df["Issue_Type"].isin(VALID_ISSUE_TYPES)]
    # filtered_df = filtered_df[RELEVANT_FIELD_MAP]

    # ticket_types = (
    #     filtered_df["Issue_Type"]
    #     .groupby(filtered_df["Project"])
    #     .value_counts()
    #     .unstack(fill_value=0)
    # )
    # print("Ticket Types Count:")
    # print(ticket_types)
    jira = setup_jira_connection()
    print(
        jira.get_(
            "Branch Ops 2025.10.1",
            maxResults=1,
            fields="*all",
            expand="changelog",
            json_result=True,
        )
    )
