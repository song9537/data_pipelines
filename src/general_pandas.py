# basic pandas pipeline utility
import pandas as pd
import logging
import pathlib

logging.basicConfig(level=logging.INFO)  # set logging level, can change later if production si good


def pandf_csv(df: pd.DataFrame, out_fp: pathlib.Path):
    """
    load pandas dataframe to csv file
    :param df:
    :param out_fp:
    :return:
    """

    if not df.empty:
        df.to_csv(path_or_buf=out_fp)

    logging.info(f'{df.head()}\n loaded to: {out_fp.__str__()}')

    return


def csv_pandf(in_fp: pathlib.Path):
    """
    extract pandas dataframe to csv file
    :param in_fp:
    :return:
    """

    df = pd.read_csv(in_fp)

    logging.info(f'{df.head()}\n loaded from: {in_fp.__str__()}')

    return df


def main():
    pass

if __name__ == '__main__':
    main()
