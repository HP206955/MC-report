import pandas as pd
import datetime
from . import monte_carlo_simulation

# Constants
TEAM_RELEASE_CADENCE = {
    "Connect Partner API": 0,  # Weekly
    "Integrations API": 0,  # Weekly
    "Integrations Platform": 1,  # Biweekly
    "Customer Management": 1,
    "Mobile": 1,
    "Order Create": 1,
    "Personalization": 1,
    "Products & Pricing": 1,
    "Integrations Enabling": 1,
    "Order Management": 1,
    "Order Submit & Ingest": 1,
    "Experience Enhancements": 1,
    "Platform Engineering": 1,
}


def get_release_info(team_name, periods):
    """Get release date and days until release for a team."""
    cadence = "Biweekly" if TEAM_RELEASE_CADENCE[team_name] else "Weekly"
    release_date = datetime.datetime.strptime(
        periods.first().loc[cadence, "release_date"], "%Y-%m-%dT%H:%M"
    ).date()

    days_until_release = abs(release_date - datetime.date.today()).days
    print(f"Next release date: {release_date}")
    print(f"Days until next release: {days_until_release}")

    return release_date, days_until_release


def process_team_forecast(
    team_name,
    team_data,
    days_until_release,
    relevant_range,
    simulations,
):
    """Process forecast data for a single team."""
    if team_name not in team_data.groups:
        return [team_name, 0, 0, days_until_release, 0]

    group = team_data.get_group(team_name).sort_values(by="date_day", ascending=False)
    historical_throughput = group["throughput"].tolist()[:relevant_range]
    print(f"Team: {team_name}")
    print(
        f"Relevant historical throughput (last {relevant_range} entries): {historical_throughput}"
    )

    current_forecast = monte_carlo_simulation.simulates(
        historical_throughput, forecast_days=days_until_release, simulations=simulations
    )

    future_forecast = monte_carlo_simulation.simulates(
        historical_throughput,
        forecast_days=7 * (TEAM_RELEASE_CADENCE[team_name] + 1),
        simulations=simulations,
    )

    return [
        team_name,
        int(future_forecast["_85_pt"]),
        int(future_forecast["_70_pt"]),
        days_until_release,
        int(current_forecast["_85_pt"]),
    ]


def get_raw_forecasted_throughput(
    throughput_csv,
    release_cadences_csv,
    relevant_range=60,
    simulations=1000,
):
    """Generate raw forecasted throughput data for all teams."""
    throughput = pd.read_csv(throughput_csv)
    release_cadences = pd.read_csv(release_cadences_csv)
    teams = throughput.groupby("team")
    periods = release_cadences.groupby("cadence")

    forecast = []
    for team_name in TEAM_RELEASE_CADENCE:
        _, days_until_release = get_release_info(team_name, periods)
        forecast.append(
            process_team_forecast(
                team_name=team_name,
                team_data=teams,
                days_until_release=days_until_release,
                relevant_range=relevant_range,
                simulations=simulations,
            )
        )

    return forecast


def get_forecasted_throughput(
    relevant_range=60,
    throughput_csv="throughput.csv",
    release_cadences_csv="release_cadences.csv",
):
    """
    Generate formatted forecasted throughput DataFrame.

    Args:
        relevant_range: Number of historical data points to consider
        throughput_csv: Path to throughput CSV file
        release_cadences_csv: Path to release cadences CSV file

    Returns:
        pd.DataFrame: Formatted forecast data sorted by 85th percentile
    """
    future_forecast = get_raw_forecasted_throughput(
        relevant_range=relevant_range,
        throughput_csv=throughput_csv,
        release_cadences_csv=release_cadences_csv,
    )
    print(f"Forecasted throughput for next release: {future_forecast}")

    df = pd.DataFrame(
        future_forecast,
        columns=[
            "team_name",
            "_85_pt",
            "_70_pt",
            "days_until_release",
            "current_period_forecast",
        ],
    )
    df.sort_values(by=["_85_pt", "_70_pt", "current_period_forecast"], inplace=True)
    return df
