import datetime
import os
import pandas as pd
import logging

from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv

from faker_gen import create_single_dataframe, create_multi_dataframe

logging.basicConfig(level=logging.INFO)  # set logging level, can change later if production si good

# load .env environment vars
env_vars = load_dotenv()
MONGO_DB_USER = os.getenv("MONGO_DB_USER")
MONGO_DB_PASS = os.getenv("MONGO_DB_PASS")
NGROK_TCP_ADDR = os.getenv("NGROK_TCP_ADDR")


def get_mongo_connection(
        endpoint: str,
        username: str = MONGO_DB_USER,
        password: str = MONGO_DB_PASS):
    """
    connect to the mongo db using user and password authentication, runs a small test of
    inserting, finding and deleting a test document to ensure operability

    :param username: username to authenticate mongodb connection
    :param password: password to authenticate mongodb connection
    :param endpoint: end point to store of fetch data ['local' or 'ngrok']
    :return:
    """

    if endpoint == 'local':
        mongo_uri = f'mongodb://{username}:{password}@localhost:27017/'
    elif endpoint == 'ngrok':
        mongo_uri = f'mongodb://{username}:{password}@{NGROK_TCP_ADDR}'
    else:
        mongo_uri = None

    try:
        client = MongoClient(mongo_uri)
        logging.info("MongoDB Client Connected")
        # mongo_test_ops(client=client)

        return client

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
        return None

    except OperationFailure as e:
        print(f"An error occurred with MongoDB: {e}")
        return None

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def mongo_test_ops(client):
    """
    test basic features of connection, ensures client can manipulate database
    :param client:
    :return:
    """
    db = client['test_database']  # Replace with your database name

    # Insert a test document
    test_collection = db['test_collection']
    test_collection.insert_one({"name": "Test"})

    # Try to find the test document
    test_collection.find_one({"name": "Test"})

    # remove test doc so they dont pile up
    client.drop_database('test_database')

    logging.basicConfig(level=logging.INFO)
    logging.info("MongoDB Client Operable")

    pass


def mongodb_pandf(db_name: str, client: MongoClient, collection_name: str = None) -> pd.DataFrame:
    """
    get mongoDB data to pandas dataframe - using pandf to designate endpoint since other libs can
    generate dataframe also

    :param db_name: name of database requested
    :param client: optional since we can default to the configured MongoDB
    :param collection_name: name of collection if not entire db
    :return:
    """

    db = client[db_name]

    # if collection specified return collection as df, else return multi-indexed df with all collections
    collection = db[collection_name]
    df = pd.DataFrame(list(collection.find()))
    logging.info(f"Loaded {collection_name} as Single Index DataFrame")

    # possible later function for returning multiple collections as multi index dataframe
    # else:
    #     dfs = {}
    #     for collection_name in db.list_collection_names():
    #         collection = db[collection_name]
    #         dfs[collection_name] = pd.DataFrame(list(collection.find()))
    #         logging.info(f"Loaded {collection_name} into Multi Index DataFrame")
    #     df = pd.concat(dfs, names=['Collection', 'Row ID'])

    return df


def pandf_mongodb(df: pd.DataFrame, db_name: str, collection_name: str, client: MongoClient):
    """
    push the pandas dataframe to mongodb
    :param df: single index only, empty dataframes will be ignored
    :param db_name:
    :param collection_name:
    :param client:
    :return:
    """

    if not df.empty:
        db = client[db_name]

        # For a single-index DataFrame, push to the named collection
        collection = db[collection_name]
        records = df.to_dict('records')  # Convert dataframe to dict
        collection.insert_many(records)  # Insert into collection

    return


def mongodb_latestdatetime(client,
                           db_name: str,
                           collection_name: str,
                           date_col: str) -> datetime.datetime:

    """
    if a collection has a date type column , get the newest entry date as datetime.datetime
    :param client: mongo client
    :param db_name: database name
    :param collection_name: collection name
    :param date_col: column containing date type info in collection
    :return:
    """

    db = client[db_name]
    collection = db[collection_name]

    try:
        latest_dt = collection.find().sort(date_col, DESCENDING).limit(1)[0]
        latest_dt = latest_dt[date_col]
    except:
        logging.info(f'No datetype information found in {db_name}:{collection_name}:{date_col}')
        latest_dt = None

    return latest_dt


def test_mongo():
    """
    test single and multi index dataframe loading and extracting for pandas pipelines
    :return:
    """

    # test local con
    # client = get_mongo_connection(endpoint='local', endpoint_addr=None)
    # single_collction_extract_load(client=client)

    # test ngrok con specified
    client = get_mongo_connection(endpoint='ngrok', endpoint_addr=NGROK_TCP_ADDR)
    # dt = mongodb_latestdatetime(client=client, db_name='coinma')
    single_collction_extract_load(client=client)

    return


def single_collction_extract_load(client):

    # test single dataframe capability
    fake_singledf = create_single_dataframe()

    db_name = 'test_database'
    collection_name = 'test_collection'

    # load pandas dataframe to mongo
    pandf_mongodb(
        client=client,
        df=fake_singledf,
        db_name=db_name,
        collection_name=collection_name
    )

    # extract pandas dataframe from mongo
    df = mongodb_pandf(
        client=client,
        db_name=db_name,
        collection_name=collection_name
    )

    client.drop_database(db_name)  # drop the test db when finished

    return


if __name__ == '__main__':
    test_mongo()
