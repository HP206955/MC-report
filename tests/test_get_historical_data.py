import unittest
from jira_connector import get_historical_data
from datetime import date


class TestGHD(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.maxDiff = None

    def test_parse_date(self):
        self.assertEqual(
            get_historical_data.parse_date("2025-10-03T12:11:14.768-0500"),
            date(2025, 10, 3),
        )

    def test_fetch_jira_issues(self):
        conn = get_historical_data.setup_jira_connection()
        issues = get_historical_data.fetch_jira_issues(
            conn, base_date=date(2025, 10, 1), max_fetch=10
        )

        self.assertIsNotNone(issues)
        self.assertIsInstance(issues, list)
        self.assertLessEqual(len(issues), 10)

    def test_process_historical_data(self):
        issues = [
            {
                "key": "PS2-1",
                "fields": {
                    "summary": "Test summary",
                    "created": "2023-01-01T12:00:00.000+0000",
                    "status": {"name": "To Do"},
                    "customfield_10000": 1,
                    "updated": "2023-01-02T12:00:00.000+0000",
                    "issuetype": {"name": "Task"},
                    "priority": {"name": "High"},
                    "fixVersions": [{"name": "R9_10162025"}],
                    "components": [{"name": "Google Analytics"}],
                    "assignee": {"displayName": "John Doe"},
                    "reporter": {"displayName": "Jane Doe"},
                    "project": {"key": "PS2"},
                    "resolution": {"name": "Unresolved"},
                    "labels": ["INT-API", "Java17Upgrade"],
                    "parent": {"key": "PS2-0"},
                    "resolutiondate": "2023-01-03T12:00:00.000+0000",
                },
            }
        ]
        expected_data = {
            "ID": "PS2-1",
            "Link": "https://abcsupply.atlassian.net/browse/PS2-1",
            "Title": "Test summary",
            "Backlog": date(2023, 1, 1),
            "Current_Status_Category": "To Do",
            "Item_Rank": 1,
            "Updated": date(2023, 1, 2),
            "Issue_Type": "Task",
            "Priority": "High",
            "Fix_versions": "R9_10162025",
            "Components": "Google Analytics",
            "Assignee": "John Doe",
            "Reporter": "Jane Doe",
            "Project": "PS2",
            "Resolution": "Unresolved",
            "Labels": "[INT-API|Java17Upgrade]",
            "Blocked_Days": "",
            "Blocked": "FALSE",
            "Parent": "PS2-0",
            "done_datetime": "2023-01-03 12:00:00.000000 UTC",
        }
        df = get_historical_data.process_historical_data(issues)
        self.assertIsNotNone(df)
        self.assertEqual(df.to_dict(orient="records")[0], expected_data)

    def test_process_status_changes(self):
        raise NotImplementedError("Test not implemented yet")
    
    def test_get_historical_data(self):
        raise NotImplementedError("Test not implemented yet")

if __name__ == "__main__":
    unittest.main(buffer=True)
