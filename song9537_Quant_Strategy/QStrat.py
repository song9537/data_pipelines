import numpy as np
import pandas as pd

# Quantitative strategies will primarily depend on the forecasted result from machine learning.
# To measure the risk of shortfall, series of technical indicators are needed.
# The goal is to create an algorithm that calculates risks simultaneously while investment strategy is taken place


def rsi(data, period):

    # data is the price of the crypto and
    # period is the interval over which the RSI is calculated

    delta = data.diff().dropna()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()
    rs = avg_gain / avg_loss return 100 - (100 / (1 + rs))
    data['RSI'] = rsi(data['Close'],14) # Calculate the 14-day RSI

def 