import unittest
from jira_connector import get_historical_data
from datetime import date


class TestGHD(unittest.TestCase):
    def test_parse_date(self):
        self.assertEqual(
            get_historical_data.parse_date("2025-10-03T14:23:00.000+0000"),
            date(2025, 10, 3),
        )

    def test_fetch_jira_issues(self):
        pass


if __name__ == "__main__":
    unittest.main()
