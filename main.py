from monte_carlo import forecasted_throughput
from jira_connector import get_historical_data, get_pivoted_data

if __name__ == "__main__":
    historical_data = get_historical_data.get(max_fetch=200)
    print(historical_data["done_datetime"])
    # historical_data.to_csv("data/jira_issues_historical.csv")
    # pivoted_df = get_pivoted_data.get(historical_csv="data/jira_issues_historical.csv")
    # pivoted_df.to_csv("data/raw_format.csv")

    forecasted_df = forecasted_throughput.get_forecasted_throughput(
        throughput_csv="data/throughput.csv",
        release_cadences_csv="data/release_cadences.csv",
        relevant_range=30,
    )
    forecasted_df.to_csv("data/raw_format_dual.csv", index=False)
