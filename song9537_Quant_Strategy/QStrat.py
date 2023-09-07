import numpy as np
import pandas as pd

# Quantitative strategies will primarily depend on the forecasted result from machine learning.
# To measure the risk of shortfall, series of technical indicators are needed.
# The goal is to create an algorithm that calculates risks simultaneously while investment strategy is taken place

def Trade_Signal(predicted_price, purchase_price):

    """
    Generates a trading signal based on the predicted price
    Second, Monitors the price changes from real time data.
    Third, if current price decreases and portfolio experience heavy loss, abort mission. If else, make money

    :param predicted_price:
    :param purchase_price:
    :return:
    """
    trade_signal = {}

    price_difference = predicted_price - purchase_price
    relative_difference = price_difference / purchase_price

    if relative_difference > 0.05:
        return 1
    elif -0.05 <= relative_difference <= 0.05:
        return 0
    else:
        return -1

def Perform_Trade(trade_signal):

    """
    Perform trade when a signal is triggered

    :param trade_signal:
    :return:
    """

    if trade_signal == 1:
        print("Trigger API to Buy")
    elif trade_signal == 0:
        print("Trigger API to Hold")
    elif trade_signal == -1:
        print("Trigger API to Sell")
    else:
        print("Invalid Signal")

def Assess_Risk(current_price, purchase_price):

    """
    Monitors the portfolio and generates a sell signal if needed.

    Parameters:
    - buy price:
    - current_prices:

    Returns:
    """
    trade_signal = {}


    # Calculate the relative decrease in price
    relative_decrease = (purchase_price - current_price) / purchase_price

    if relative_decrease >= 0.10:
        trade_signal = -1
    else:
        trade_signal = 1