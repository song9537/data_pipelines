import os
import pandas as pd
import logging

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from dotenv import load_dotenv

from faker_gen import create_single_dataframe, create_multi_dataframe

logging.basicConfig(level=logging.INFO)  # set logging level, can change later if production si good

# load .env environment vars
env_vars = load_dotenv()
MONGO_DB_USER = os.getenv("MONGO_DB_USER")
MONGO_DB_PASS = os.getenv("MONGO_DB_PASS")


def get_mongo_connection(username: str = MONGO_DB_USER, password: str = MONGO_DB_PASS):
    """
    connect to the mongo db using user and password authentication, runs a small test of
    inserting, finding and deleting a test document to ensure operability

    :param username: username to authenticate mongodb connection
    :param password: password to authenticate mongodb connection
    :return:
    """

    mongo_uri = f'mongodb://{username}:{password}@localhost:27017/'

    try:
        client = MongoClient(mongo_uri)
        mongo_test_ops(client=client)

        return client

    except ConnectionFailure:
        print("Could not connect to MongoDB")
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
    test_collection.delete_one({"name": "Test"})

    logging.basicConfig(level=logging.INFO)
    logging.info("MongoDB Client Connected")

    pass


def mongodb_pandf(db_name: str, client: MongoClient = None, collection_name: str = None) -> pd.DataFrame:
    """
    get mongoDB data to pandas dataframe - using pandf to designate endpoint since other libs can
    generate dataframe also

    :param db_name: name of database requested
    :param client: optional since we can default to the configured MongoDB
    :param collection_name: name of collection if not entire db
    :return:
    """

    if not client:  # get default client if not passed explicitly
        client = get_mongo_connection()

    db = client[db_name]

    # if collection specified return collection as df, else return multi-indexed df with all collections
    if collection_name:
        collection = db[collection_name]
        df = pd.DataFrame(list(collection.find()))
        logging.info(f"Loaded {collection_name} as Single Index DataFrame")
    else:
        dfs = {}
        for collection_name in db.list_collection_names():
            collection = db[collection_name]
            dfs[collection_name] = pd.DataFrame(list(collection.find()))
            logging.info(f"Loaded {collection_name} into Multi Index DataFrame")
        df = pd.concat(dfs, names=['Collection', 'Row ID'])

    return df


def pandf_mongodb(df: pd.DataFrame, db_name: str, collection_name: str = None, client: MongoClient = None):
    """
    push the pandas dataframe to mongodb
    :param df: can be single or multi index
    :param db_name:
    :param collection_name:
    :param client:
    :return:
    """

    if not client:  # get default client if not passed explicitly
        client = get_mongo_connection()

    db = client[db_name]

    if collection_name is not None:
        # For a single-index DataFrame, push to the named collection
        collection = db[collection_name]
        records = df.to_dict('records')  # Convert dataframe to dict
        collection.insert_many(records)  # Insert into collection
    else:
        # For a multi-index DataFrame, push to the appropriate collections
        for name, sub_df in df.groupby(level=0):
            collection = db[name]
            records = sub_df.droplevel(0).to_dict('records')  # Drop the collection level and convert to dict
            collection.insert_many(records)  # Insert into collection

    return


def test_mongo():
    """
    test single and multi index dataframe loading and extracting for pandas pipelines
    TODO finish multi indexing
    :return:
    """

    client = get_mongo_connection()

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
