# this is trash and Im not using it rn
import pandas as pd
from pycoingecko import CoinGeckoAPI


def coingeckoapi_df(coin, days, currency='cad'):
    """
    god left me unfinished
    :return:
    """
    cg = CoinGeckoAPI()
    allcoins_dict = cg.get_coins_list()
    allcoins_list = [coin['id'] for coin in allcoins_dict]

    if coin:
        if coin not in allcoins_list:
            raise ValueError("Non-existant coin in coins list supplied")

    data = cg.get_coin_market_chart_by_id(
        id=coin,
        vs_currency=currency,
        days=days
    )

    df = pd.DataFrame(data)
    df['prices'] = df['prices'].apply(lambda x: x[1])
    df['market_caps'] = df['market_caps'].apply(lambda x: x[1])
    df['total_volumes'] = df['total_volumes'].apply(lambda x: x[1])

    return df


def main():

    df = coingeckoapi_df(coin='aave', days=500)
    # df = cryptocmd()
    print(df.head())
    print()


if __name__ == '__main__':
    main()