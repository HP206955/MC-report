"""
Monte Carlo Simulation Module for Throughput Forecasting.

This module implements Monte Carlo simulation techniques to forecast future throughput
based on historical data. It uses random sampling from historical throughput data
to generate multiple simulations and calculate probability-based forecasts.

The simulation accounts for variability in daily throughput and provides confidence
intervals (70th and 85th percentiles) for the expected total throughput over a
specified forecast period.
"""

import random
import numpy as np


def simulates(historical_throughput, forecast_days=14, simulations=1000):
    """
    Run Monte Carlo simulations to forecast future throughput.

    Uses historical throughput data to generate multiple simulations of future
    throughput over a specified period. Each simulation randomly samples from
    the historical data to create daily forecasts.

    Args:
        historical_throughput: List of historical daily throughput values
        forecast_days: Number of days to forecast into the future (default: 14)
        simulations: Number of Monte Carlo simulations to run (default: 1000)

    Returns:
        Dict containing two forecast values:
            - _70_pt: 30th percentile forecast (70% chance of exceeding this value)
            - _85_pt: 15th percentile forecast (85% chance of exceeding this value)

    Example:
        >>> historical_data = [3, 5, 0, 2, 4, 1, 3]
        >>> result = simulates(historical_data, forecast_days=7, simulations=1000)
        >>> print(result)
        {'_70_pt': 15.0, '_85_pt': 12.0}
    """
    all_forecasts = []
    for _ in range(simulations):
        daily_forecast = []
        for _ in range(forecast_days):
            # Sample directly from all historical data (including zeros)
            daily_throughput = random.choice(historical_throughput)
            daily_forecast.append(daily_throughput)
        # Calculate total for this simulation
        total_throughput = sum(daily_forecast)
        all_forecasts.append(total_throughput)
    # Calculate only the requested percentiles
    results = {
        "_70_pt": np.percentile(
            all_forecasts, 30
        ),  # There’s a 70% chance we’ll exceed this number
        "_85_pt": np.percentile(
            all_forecasts, 15
        ),  # There’s an 85% chance we’ll exceed this number
    }
    return results
