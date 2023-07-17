# this is also trash and Im not using it rn
# might be useful for finding new coins
# otherwise cant get the resolutions needed for good modelling without paying for the API
import os
import logging


import pandas as pd
import requests
import datetime as dt
from dotenv import load_dotenv
from cryptocmd import CmcScraper
from coinmarketcapapi import CoinMarketCapAPI

env = load_dotenv()
COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')
COINMARKETCAP_DATABASE_NAME = os.getenv('COINMARKETCAP_DATABASE_NAME')

logging.basicConfig(level=logging.INFO)  # set logging level, can change later if production si good


def cmc_get_listed_coins() -> pd.DataFrame:
    """
    get all of the coins listed on coinmarketcap in a dataframe
    :return:
    """
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/map'

    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': COINMARKETCAP_API_KEY,  # Replace with your API key
    }

    response = requests.get(url, headers=headers)
    response_json = response.json()

    coins = response_json['data']
    df = pd.DataFrame.from_dict(coins)
    df.drop(['id'], axis=1, inplace=True)
    df.sort_values(['rank'], inplace=True)  # sort by rank?

    return df


def cmclatest_pandf(coin_code: str,
                    currency: str = 'USD') -> pd.DataFrame:
    """
    get the latest currency quote from coinmarketcap
    :param coin_code: symbol from coinmarket cap crypto map endpoint
    :param currency: supported string for currency from coinmarketcap
    :return:
    """

    cmc = CoinMarketCapAPI(COINMARKETCAP_API_KEY)
    rep = cmc.cryptocurrency_quotes_latest(symbol=coin_code, convert=currency)
    data = rep.data[coin_code][0]['quote'][currency]
    df = pd.DataFrame([data])

    logging.info(f"Extracted {coin_code}:latest")

    return df


def cmchist_pandf(
        coin_code: str,
        start_date: dt.datetime = None,
        end_date: dt.datetime = None) -> pd.DataFrame:
    """
    fetch historical data for coin specified by coin_symbol by scraping coinmarketcap

    :param coin_code coin code must exist in the coinmarketcap crypto map
    :param start_date datetime, must be before end_date, must be passed with end_date
    :param end_date datetime, must be after start_date, must be passed with start_date
    :return:
    """

    start = None
    end = None
    all_time_flag = True
    if start_date and end_date:  # need both start date and end date
        start = start_date.strftime("%d-%m-%Y")
        end = end_date.strftime("%d-%m-%Y")
        all_time_flag = False

    # initialise scraper without time interval
    scraper = CmcScraper(
        coin_code=coin_code,
        start_date=start,
        end_date=end,
        all_time=all_time_flag
    )

    # Pandas dataFrame for the same data
    df = scraper.get_dataframe()

    return df


# def cmc_update_coin(
#         coin_code: str,
#         endpoint):
#     """
#     grab the latest entry from the collection if exists, get all newer info from cmc, push to endpoint
#     :param coin_code:
#     :param endpoint: storage endpoint
#     :param client: mongodb client
#     :return:
#     """
#
#     client = get_mongo_connection(endpoint=endpoint)
#
#     latest_dt = mongodb_latestdatetime(
#         client=client,
#         db_name=COINMARKETCAP_DATABASE_NAME,
#         collection_name=coin_code,
#         date_col='Date')
#
#     end_date = None
#     if latest_dt:
#         end_date = dt.datetime.today()
#
#     df = cmchist_pandf(coin_code=coin_code, start_date=latest_dt, end_date=end_date)
#     df = df[df['Date'] != latest_dt]
#
#     cmcpandf_mongodb(df=df, coin_code=coin_code, client=client)
#
#     return
#
#
# def mongodb_cmcpandf(
#         coin_code: str,
#         client) -> pd.DataFrame:
#     """
#     get a pandas dataframe with coinmarketcap data from a mongodb instance
#     :param coin_code: coin code available on coinmarketcap
#     :param client: mongodb client
#     :return:
#     """
#
#     df = mongodb_pandf(
#         client=client,
#         db_name=COINMARKETCAP_DATABASE_NAME,
#         collection_name=coin_code
#     )
#
#     # _id is an artifact of the mongodb system, in this instance where duplicate documents should not
#     # exist in the db, it should not be required
#     df.drop(['_id'], inplace=True, axis=1)
#
#     return df
#
#
# def cmcpandf_mongodb(
#         df: pd.DataFrame,
#         coin_code: str,
#         client):
#     """
#     push coinmarketcap pandas dataframe to a mongodb instance
#     :param df: dataframe to push
#     :param coin_code: coin code from coinmarketcap crypto map
#     :param client: mongodb client
#     :return:
#     """
#
#     pandf_mongodb(
#         df=df,
#         db_name=COINMARKETCAP_DATABASE_NAME,
#         collection_name=coin_code,
#         client=client
#     )
#
#     return


def main():

    # df = cmc_get_listed_coins()
    # coin_symbol = df.loc[df['rank'] == 10]
    # coin_symbol = coin_symbol['symbol'].values[0]
    #
    # cmc_update_coin(coin_code=coin_symbol, endpoint='local')

    cmclatest_pandf(coin_code='BTC')

    print()


if __name__ == '__main__':
    main()