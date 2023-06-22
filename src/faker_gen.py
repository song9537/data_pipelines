# generate some fake dataframes for testing

from faker import Faker
import pandas as pd
import numpy as np
import logging

fake = Faker()
logging.basicConfig(level=logging.INFO)  # set logging level, can change later if production si good


# Single-index DataFrame
def create_single_dataframe(n: int = 100):
    """
    Create fake single index dataframe of size n
    :param n:
    :return:
    """
    df = {
        "name": [fake.name() for _ in range(n)],
        "email": [fake.email() for _ in range(n)],
        "city": [fake.city() for _ in range(n)],
    }

    df = pd.DataFrame(df)
    logging.info(f'Created fake dataframe for testing \n{df.head()}')

    return df


# Multi-index DataFrame
def create_multi_dataframe(n: int = 100, sub_df_names: list = None):

    if not sub_df_names:
        sub_df_names = ['collection1', 'collection2', 'collection3']

    df = {
        "df_name": np.repeat(sub_df_names, n),
        "name": [fake.name() for _ in range(n*len(sub_df_names))],
        "email": [fake.email() for _ in range(n*len(sub_df_names))],
        "city": [fake.city() for _ in range(n*len(sub_df_names))],
    }
    df = pd.DataFrame(df)
    logging.info(f'Created fake dataframe for testing \n{df.head()}')
    df.set_index('df_name', inplace=True)
    return df
