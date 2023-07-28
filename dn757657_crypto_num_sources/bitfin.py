# functions for extracting data from bitfinex API
import datetime
import bitfinex
import requests
import logging
import pytz
import time
import os

import pandas as pd
import datetime as dt

from dotenv import load_dotenv

env = load_dotenv()
BITFIN_DB_NAME = os.getenv('BITFIN_DB_NAME')

logging.basicConfig(level=logging.INFO)  # log INFO statements to console

BITFINEX_RESOLUTIONS = ["1m", "5m", "15m", "30m", "1h", "3h", "6h", "12h", "1D", "1W", "14D"]


def bitfin_get_listed_pairs() -> pd.DataFrame:
    """
    All assets on the bitfinex exchange are listed as trading pairs. Pairs exist in various formats:
        -   XXXYYY where XXX is the asset symbol and YYY is the comparison asset symbol e.g. BTCUSD represents
            bitcoin price in US dollars
        -   XXXX:YYY where XXXX is the asset symbol and YYY is the comparison asset symbol, this format is used
            when the asset symbol character length is greater than 3
    :return: pandas dataframe of trading pairs structured ['bitfinex_pairs', 'symbol', 'currency']
    """

    url = "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange"

    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    response = response.text.replace('[', "")
    response = response.replace(']', "")
    response = response.replace(',', "")
    response = response.split('"')

    pairs = [x for x in response if len(x) != 0]

    pairs_col = 'bitfin_pairs'
    df = pd.DataFrame(pairs, columns=[pairs_col])

    df['currency'] = df[pairs_col].apply(currency_from_bitfinpair)
    df['symbol'] = df[pairs_col].apply(symbol_from_bitfinpair)

    return df


def bitfininterval_timedelta(interval: str) -> datetime.timedelta:
    """
    Create bitfinex API compatible timedelta type object from a string
    :param interval: string indicating bitfinex compatible time interval
    :return: datetime.timedelta object
    """

    if interval in BITFINEX_RESOLUTIONS:
        interval = pd.Series([interval])
        interval = pd.to_timedelta(interval)
        interval = interval[0]
    else:
        logging.info(f'Requested interval {interval} incompatible with bitfinex API\n'
                     f'Please select from:\n {BITFINEX_RESOLUTIONS.__repr__()}')

    return interval


# TODO something is wrong with this if used when batching is not required
def bitfinbatch_pandf(pair_code: str,
                      interval: str = '1m',
                      limit: int = 10000,
                      tz: pytz.timezone = pytz.timezone('America/Halifax'),
                      end: dt.datetime = dt.datetime.now(),
                      start: dt.datetime = None) -> pd.DataFrame:
    """
    Fetch bitfinex data from API endpoint in batches using start and end dates at the desired resolution, batches are
    required as the bitfinex API will return a maximum of ten thousand records per request. If the limit is exceeded
    in any given request, records starting and the end date and working backwards will be returned. Up to the end
    date will always be returned but not always back to the start date.

    :param pair_code: string trading pair code compatible with bitfienx
    :param interval: string time interval compatible with bitfinex API
    :param limit: default is max @ 10000
    :param tz: timezone to make dates work right because api is kind of stupid
    :param start: dates should be passed in utc, as data is fetched as utc
    :param end: dates should be passed in utc, as data is fetched as utc
    :return: pd.Dataframe containing market data for trading pair
    """

    interval_delta = bitfininterval_timedelta(interval)

    # need to localize so dates line up when requested from api
    if start:
        start = start.replace(tzinfo=pytz.UTC).astimezone(tz)
    if end:
        end = end.replace(tzinfo=pytz.UTC).astimezone(tz)

    df = 0
    while not isinstance(df, pd.DataFrame):

        df = bitfin_pandf(pair_code=pair_code,
                          interval=interval,
                          limit=limit,
                          start=start,
                          end=end
                          )

    if start and end:  # complete dataframe with dates requested
        while pytz.utc.localize(df['time'].min()) > start:

            end = pytz.utc.localize(df['time'].min()) - interval_delta
            df_new = bitfin_pandf(pair_code=pair_code,
                                  interval=interval,
                                  limit=limit,
                                  start=start,
                                  end=end)  # by default sorts newest to top, oldest last

            if not isinstance(df_new, pd.DataFrame):
                continue

            df_new = pd.concat([df, df_new])
            df_new.drop_duplicates(inplace=True)

            if df_new.equals(df):
                # if dataframe has not changed then quit, no more info avail
                break
            else:
                df = df_new

    logging.info(f"Extracted {len(df)} records of {pair_code} from {df['time'].min().__str__()} -> {df['time'].max().__str__()}")

    if not df.empty:
        df = bitfinex_renamecols(df=df, pair_code=pair_code)

    return df


def bitfinex_renamecols(df: pd.DataFrame,
                        pair_code: str,
                        source: str = BITFIN_DB_NAME):
    logging.info(df.head())

    df = df.add_prefix(pair_code + "_")
    df = df.add_prefix(source + "_")

    return df


# TODO reorganize batch and this func into a master and slave setup
def bitfin_pandf(pair_code: str,
                 interval: str = '1m',
                 limit: int = 10000,
                 start: dt.datetime = None,
                 end: dt.datetime = None) -> pd.DataFrame:
    """
    start not required with end, if request is larger than limit, records are pulled by starting at end date and working
    backwards
    defaults to end as now
    limit 10000 max
    90 req/min limit
    everything has to be in utc since everything is fetched in utc, convert datetimes to utc?
    :param pair_code: string trading pair code compatible with bitfienx
    :param interval: string time interval compatible with bitfinex API
    :param limit: default is max @ 10000
    :param start: dates should be passed in utc, as data is fetched as utc
    :param end: dates should be passed in utc, as data is fetched as utc
    :return: pd.Dataframe containing market data for trading pair
    :return:
    """

    if start:  # api requires unix millisecond timestamps
        start_unix_ms = int(datetime.datetime.timestamp(start) * 1000)
    else:
        start_unix_ms = None

    if end:
        end_unix_ms = int(datetime.datetime.timestamp(end) * 1000)
    else:
        end_unix_ms = None

    api_v2 = bitfinex.bitfinex_v2.api_v2()
    cols = ['time', 'open', 'close', 'high', 'low', 'volume']

    result = api_v2.candles(symbol=pair_code, interval=interval,
                            limit=limit, start=start_unix_ms,
                            end=end_unix_ms)
    try:
        df = pd.DataFrame(result, columns=cols)
        df.drop_duplicates(inplace=True)
        df.sort_values('time')  # by default sorts newest to top, oldest last

        df['time'] = pd.to_datetime(df['time'], unit='ms')
        logging.info(f"Extracted {pair_code} from {df['time'].min().__str__()} -> {df['time'].max().__str__()}")
        time.sleep(1)

    except ValueError:  # might not be a long term solution
        logging.info(f'Reached rate limit, waiting and retrying')
        df = 0
        time.sleep(3)

    return df


def currency_from_bitfinpair(bitfin_pair: str):
    """
    bitfin defines each listing as a trading pair, often it is useful to extract the currency
    from the trading symbol
    if the asset symbol is 3 characters the trading pair is simply XXXYYY where XXX is the symbol
    and YYY is the currency, otherwise if the symbol is 4 characters or greater, the format is
    XXXX:YYY
    :param bitfin_pair bitfin pair string
    :return: currency as string
    """

    if ':' in bitfin_pair:
        currency = bitfin_pair.split(':')[1].lower()
    else:
        currency = bitfin_pair[3::].lower()

    return currency


def symbol_from_bitfinpair(bitfin_pair: str):
    """
    bitfin defines each listing as a trading pair, often it is useful to extract the currency
    from the trading symbol
    if the asset symbol is 3 characters the trading pair is simply XXXYYY where XXX is the symbol
    and YYY is the currency, otherwise if the symbol is 4 characters or greater, the format is
    XXXX:YYY
    :param bitfin_pair bitfin pair string
    :return: symbmol as string
    """

    if ':' in bitfin_pair:
        symbol = bitfin_pair.split(':')[0].lower()
    else:
        symbol = bitfin_pair[0:3].lower()

    return symbol


# def main():
#
#     local_tz = pytz.timezone('America/Halifax')
#
#     start = dt.datetime.strptime('01-01-2022', '%d-%m-%Y')
#     # start = start.replace(tzinfo=pytz.UTC).astimezone(local_tz)
#
#     end = dt.datetime.strptime('01-02-2022', '%d-%m-%Y')
#     # end = end.replace(tzinfo=pytz.UTC).astimezone(local_tz)
#
#     df = bitfinbatch_pandf('btcusd', start=start, end=end)
#
#     bitfin_get_listed_pairs()
#
#
# if __name__ == '__main__':
#     main()