"""
This module processes and transforms Jira issue data for Monte Carlo simulations.
It provides functionality to pivot and format Jira issue data, mapping status categories
and column names to standardized formats. The module handles filtering of valid issue
types and transforms raw Jira data into a format suitable for throughput analysis
and Monte Carlo simulations.
"""

import pandas as pd

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
    "Status Change Date",
    "Status Change From",
    "Status Change To",
]

VALID_ISSUE_TYPES = ["Story", "Bug", "Defect", "Production Support"]

COLUMN_NAME_MAPPING = {
    "In Refinement": "In_Refinement",
    "Ready": "Ready",
    "In Progress": "In_Progress",
    "In Review": "In_Review",
    "Ready for QA": "Ready_for_QA",
    "In QA": "In_QA",
    "Done": "Done",
}

STATUS_CATEGORY_MAPPING = {
    "Backlog": "Backlog",
    "Selected for Development": "Backlog",
    "To Do": "Backlog",
    "Ready for Analysis": "Backlog",
    "In Analysis": "Backlog",
    "Ready to Deploy": "Done",
    "Done": "Done",
    "Ready for Production": "Done",
    "Internally Reviewed": "In Progress",
    "In Progress": "In Progress",
    "Locally Tested": "In Progress",
    "Ready for Review": "In Progress",
    "Blocked": "In Progress",
    "Development In Progress": "In Progress",
    "In QA": "In QA",
    "Ready for Sign-off": "In QA",
    "Test In Progress": "In QA",
    "In Refinement": "In Refinement",
    "For Analysis": "In Refinement",
    "In Review": "In Review",
    "Peer Review": "In Review",
    "Code Review": "In Review",
    "IN PR": "In Review",
    "Ready For Development": "Ready",
    "Ready": "Ready",
    "Sprint Ready": "Ready",
    "Ready for QA": "Ready for QA",
    "Rejected": "Rejected",
    "won't do": "Won't Do",
}


def filter_and_transform_data(df):
    """Filter and transform the dataframe with initial conditions."""
    df = df[df["Project"].isin(RELEVANT_PROJECT_KEYS)]

    df = df[RELEVANT_FIELD_MAP]

    df = df[df["Issue_Type"].isin(VALID_ISSUE_TYPES)]

    base_date = pd.Timestamp.today() - pd.DateOffset(months=18)
    df = df[df["Updated"] >= base_date]

    df["Current_Status_Category"] = df["Current_Status_Category"].replace(
        STATUS_CATEGORY_MAPPING
    )
    df["Status Change From"] = df["Status Change From"].replace(STATUS_CATEGORY_MAPPING)
    df["Status Change To"] = df["Status Change To"].replace(STATUS_CATEGORY_MAPPING)
    df["Current_Status_Category"] = (
        df["Current_Status_Category"].str.upper().str.replace(" ", "_")
    )
    return df


def create_pivot_table(df):
    """Create pivot table from filtered data."""
    valid_statuses = list(COLUMN_NAME_MAPPING.keys())
    df = df[df["Status Change To"].isin(valid_statuses)]

    return (
        pd.pivot_table(
            df,
            index="ID",
            columns="Status Change To",
            values="Status Change Date",
            aggfunc="max",
            fill_value="",
        )
        .reset_index()
        .rename_axis(None, axis=1)
        .rename(columns=COLUMN_NAME_MAPPING)
    )


def get(historical_csv):
    """
    Process JIRA data and return pivoted dataframe.

    Args:
        csv_path: Path to the CSV file containing JIRA data

    Returns:
        pd.DataFrame: Processed and pivoted dataframe
    """
    # Read and process data
    df = pd.read_csv(
        historical_csv, parse_dates=["Status Change Date", "Updated"], low_memory=False
    )
    df = filter_and_transform_data(df.copy())
    historical_df = df.copy()
    # Create pivot table
    pivoted_df = create_pivot_table(df)

    # Merge with historical data
    result_df = pd.merge(
        historical_df.drop(
            ["Status Change Date", "Status Change From", "Status Change To"], axis=1
        ).drop_duplicates(),
        pivoted_df,
        on="ID",
        how="left",
    )

    print(f"Total unique issues after pivot: {len(result_df['ID'].unique())}")
    return result_df
