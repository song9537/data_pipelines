# functions for interfacing with MongoDB
import datetime
import os
import logging
import pandas as pd
import pathlib

from dotenv import load_dotenv
from typing import Literal
from pymongo import MongoClient, DESCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from airflow.providers.mongo.hooks.mongo import MongoHook


logging.basicConfig(level=logging.INFO)  # set logging level, can change later if production si good

# load .env environment vars
env_vars = load_dotenv()
MONGO_DB_USER = os.getenv("MONGO_DB_USER")
MONGO_DB_PASS = os.getenv("MONGO_DB_PASS")
NGROK_TCP_ADDR = os.getenv("NGROK_TCP_ADDR")
TAILSCALE_HOST_IP = os.getenv("TAILSCALE_HOST_IP")
MODELLING_DB_NAME = os.getenv("MODELLING_DB_NAME")


def get_mongo_connection(
        endpoint: str,
        host: Literal["system", "apache-airflow"] = 'system',
        username: str = MONGO_DB_USER,
        password: str = MONGO_DB_PASS):
    """
    create a mongo connection/client
    connect to the mongo db using user and password authentication through various endpoints

    :param username: username to authenticate mongodb connection
    :param password: password to authenticate mongodb connection
    :param host: host system connecting to the mongodb, airflow has different methods of connecting
    :param endpoint: end point to store of fetch data ['local' or 'ngrok']
    :return:
    """
    hosts = ["system", "apache-airflow"]
    if host not in hosts:
        raise AttributeError(f"host must be in: {hosts.__repr__()}")

    if endpoint == 'local':
        mongo_uri = f'mongodb://{username}:{password}@localhost:27017/'
    elif endpoint == 'local-docker':
        mongo_uri = f'mongodb://{username}:{password}@host.docker.internal:27017/'
    elif endpoint == 'ngrok':
        mongo_uri = f'mongodb://{username}:{password}@{NGROK_TCP_ADDR}'
    elif endpoint == 'tailscale':
        mongo_uri = f'mongodb://{username}:{password}@{TAILSCALE_HOST_IP}:27017'
    else:
        mongo_uri = None

    try:
        if host == 'system':
            client = MongoClient(mongo_uri)

        elif host == 'apache-airflow':
            hook = MongoHook(conn_id=endpoint)
            client = hook.get_conn()

        logging.info(f"Connected to MongoDB - {client.server_info()}")

        return client

    # outline some basic exceptions
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
        return None

    except OperationFailure as e:
        print(f"An error occurred with MongoDB: {e}")
        return None

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def pandf_mongodb(data: pd.DataFrame,
                  db_name: str,
                  collection_name: str,
                  mongodb_client: MongoClient, ):
    """
    Push a dataframe to the Mongo Database
    All datasets of a given format should be included in a single database, delineated by collections
    :param data: pandas dataframe to push to db single index only, empty dataframes will be ignored
    :param db_name: string name of the database to push data into
    :param collection_name: string name of the collection to push data into
    :param mongodb_client: mongo client to connect to
    :return:
    """

    if not data.empty:
        # mongodb_client = get_mongo_connection(endpoint=endpoint)
        db = mongodb_client[db_name]

        # For a single-index DataFrame, push to the named collection
        collection = db[collection_name]
        records = data.to_dict('records')  # Convert dataframe to dict
        collection.insert_many(records)  # Insert into collection

        logging.info(f"Loaded {len(data)} Records into MongoDB:{db_name}:{collection_name}")

    return


def dict_mongodb(data: dict,
                 db_name: str,
                 collection_name: str,
                 mongodb_client: MongoClient, ):
    """
    Push a dataframe to the Mongo Database
    All datasets of a given format should be included in a single database, delineated by collections
    :param data: pandas dataframe to push to db single index only, empty dataframes will be ignored
    :param db_name: string name of the database to push data into
    :param collection_name: string name of the collection to push data into
    :param mongodb_client: mongo client to connect to
    :return:
    """

    if data:
        # mongodb_client = get_mongo_connection(endpoint=endpoint)
        db = mongodb_client[db_name]

        # For a single-index DataFrame, push to the named collection
        collection = db[collection_name]
        collection.insert_one(data)  # Insert into collection

        logging.info(f"Loaded {len(data)} Records into MongoDB:{db_name}:{collection_name}")

    return


def mongodb_pandf(db_name: str,
                  mongodb_client: MongoClient,
                  sort_by: str = 'field',
                  sort_dir: int = DESCENDING,
                  limit: int = -1,
                  collection_name: str = None) -> pd.DataFrame:
    """
    Fetch data from Mongo Database as a pandas dataframe
    get mongoDB data to pandas dataframe - using pandf to designate endpoint since other libs can
    generate dataframe also

    :param db_name: name of database requested
    :param mongodb_client: mongo client to connect to
    :param limit: limit the number of returned entries
    :param sort_by: field to sort returned data by
    :param sort_dir: [1: asc, -1: desc] direction to sort data given sort_by, does nothing if sort_by not included
    :param collection_name: name of collection if not entire db
    :return: data as pandas df from mongodb
    """

    # mongodb_client = get_mongo_connection(endpoint=endpoint)
    db = mongodb_client[db_name]

    # if collection specified return collection as df, else return multi-indexed df with all collections
    collection = db[collection_name]

    if limit == -1:
        # get sorted and limited collection from mongo
        df = pd.DataFrame(list(collection.find().sort(sort_by, sort_dir)))
    else:
        df = pd.DataFrame(list(collection.find().sort(sort_by, sort_dir).limit(limit)))

    df = mongodb_generaltransform(df=df, db_name=db_name, collection_name=collection_name)

    logging.info(f"Loaded {len(df)} Records from MongoDB:{db_name}:{collection_name} as Single Index DataFrame")

    return df


def mongodb_parquet(db_name: str,
                    path: pathlib.Path,
                    mongodb_client: MongoClient,
                    sort_by: str = 'field',
                    sort_dir: int = DESCENDING,
                    limit: int = -1,
                    collection_name: str = None) -> pd.DataFrame:
    """
    Fetch data from Mongo Database as a pandas dataframe
    get mongoDB data to parquet in FS

    :param db_name: name of database requested
    :param path: Path type object pointing to file destination
    :param mongodb_client: mongo client to connect to
    :param limit: limit the number of returned entries
    :param sort_by: field to sort returned data by
    :param sort_dir: [1: asc, -1: desc] direction to sort data given sort_by, does nothing if sort_by not included
    :param collection_name: name of collection if not entire db
    :return: data as pandas df from mongodb
    """

    df = mongodb_pandf(
        db_name=db_name,
        collection_name=collection_name,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        mongodb_client=mongodb_client
    )

    df.to_parquet(path=path)

    logging.info(f"Loaded MongoDB:{db_name}:{collection_name}:{df[0].size} Records as Parquet to: {path.__repr__()}")

    return df


def mongodb_generaltransform(df: pd.DataFrame,
                             db_name: str,
                             collection_name: str):
    """
    drop id column created by mongo
    name df columns per the database and collection names
    :return:
    """

    df = df.drop('_id', axis=1)  # drop the _id col constructed by mongo

    return df


def mongodb_latestdatetime(mongodb_client: MongoClient,
                           db_name: str,
                           collection_name: str,
                           time_col: str) -> datetime.datetime:

    """
    if a collection has a date type column , fetch the newest entry date as datetime.datetime object

    :param mongodb_client: mongo endpoint to use
    :param db_name: string database name
    :param collection_name: string collection name
    :param date_col: string column containing date type info in collection
    :return:
    """
    db = mongodb_client[db_name]
    collection = db[collection_name]

    try:
        latest_dt = collection.find().sort(time_col, DESCENDING).limit(1)[0]
        latest_dt = latest_dt[time_col]
        logging.info(f'Latest datetype object from {db_name}.{collection_name}.{time_col}:{latest_dt.__str__()}')
    except:
        logging.info(f'No datetype objects found in {db_name}:{collection_name}:{time_col}')
        latest_dt = None

    return latest_dt


def delete_top_n_entries(mongodb_client, db_name, collection_name, column_name, n, order=DESCENDING):
    """
    delete the first n entries given sort parameters
    :param mongodb_client: mongo database endpoint to connect to
    :param db_name: string database name to delete from
    :param collection_name: string name of collection to delete from
    :param column_name: string name of column to sort by
    :param n: int number of records to delete
    :param order:
    :return:
    """

    db = mongodb_client[db_name]
    collection = db[collection_name]

    # Fetch the top n entries
    cursor = collection.find().sort(column_name, order).limit(n)

    # Collect all the ids of the documents to delete
    ids_to_delete = [document['_id'] for document in cursor]

    # Delete the documents
    result = collection.delete_many({'_id': {'$in': ids_to_delete}})

    logging.info(f'Deleted {result.deleted_count} documents')

    return


def mongodb_dropdups(mongodb_client: MongoClient,
                     db_name: str,
                     collection_name: str,):
    """
    this isnt done dont use it - need to pull entire collection and use pandas index to delete duplicates
    :param mongodb_client:
    :param db_name:
    :param collection_name:
    :return:
    """

    db = mongodb_client[db_name]

    df = mongodb_pandf(
        db_name=db_name,
        collection_name=collection_name,
        mongodb_client=mongodb_client
    )

    df.drop_duplicates(keep='first', inplace=True)
    db.drop_collection(collection_name)

    pandf_mongodb(
        data=df,
        db_name=db_name,
        collection_name=collection_name,
        mongodb_client=mongodb_client
    )

    return


# some deprecated stuff
# def mongodb_models(mongodb_client: MongoClient,
#                    source: str,
#                    target_lbl: str,
#                    db_name: str = MODELLING_DB_NAME,
#                    sort_by: str = 'created_utc',
#                    sort_dir: int = DESCENDING):
#     """
#     store model and important attributes in mongo db by serializing model
#     :param source: system or library used to generate the model
#     :param mongodb_client: mongo connection to use
#     :param db_name: string name of database containing models
#     :param target_lbl: string label of model target
#     :param sort_by: document attribute to sort found mdoels by
#     :param sort_dir: direction to sort found entries
#     :return: list of models
#     """
#     models = []
#     db = mongodb_client[MODELLING_DB_NAME]
#
#     # Use GridFS for retrieving the binary file
#     fs = gridfs.GridFS(db)
#
#     # Get the pickled model and unpickle it
#     if sort_by and sort_dir:
#         files = fs.find(
#             {'source': source,
#              'target_lbl': target_lbl}).sort(sort_by, sort_dir)
#     else:
#         files = fs.find(
#             {'source': source,
#              'target_lbl': target_lbl})
#
#     for file in files:
#         pickled_model = file.read()
#         model = pickle.loads(pickled_model)
#         models.append(model)
#     else:
#         print(f"No model in {db_name}:{source} with target {target_lbl}")
#
#     return models
#
#
# def model_mongodb(model,
#                   source: str,
#                   target_lbl: str,
#                   feature_lbls: list,
#                   upper_training_bound: dt.datetime,
#                   lower_training_bound: dt.datetime,
#                   mongodb_client: MongoClient,):
#     """
#     store model and important attributes in mongo db by serializing model
#     :param source: system or library used to generate the model
#     :param mongodb_client: mongo connection to use
#     :param target_lbl: string label of model target
#     :param feature_lbls: list of string labels of training features
#     :param upper_training_bound: upper date type bound of training features
#     :param lower_training_bound: lower date type bound of training features
#     :param model: serializable model to store
#     :return:
#     """
#
#     db = mongodb_client[MODELLING_DB_NAME]
#
#     # Use GridFS for storing the binary file
#     fs = gridfs.GridFS(db)
#
#     # Pickle the model and save it to GridFS
#     pickled_model = pickle.dumps(model)
#     fs.put(
#         pickled_model,
#         source=source,
#         created_utc=dt.datetime.timestamp(dt.datetime.now()),
#         target_lbl=target_lbl,
#         training_lbls=feature_lbls,
#         upper_traning_bound=upper_training_bound,
#         lower_training_bound=lower_training_bound
#     )
#
#     return


# def mongo_test_ops(client):
#     """
#     test basic features of connection, ensures client can manipulate database
#     :param client:
#     :return:
#     """
#     db = client['test_database']  # Replace with your database name
#
#     # Insert a test document
#     test_collection = db['test_collection']
#     test_collection.insert_one({"name": "Test"})
#
#     # Try to find the test document
#     test_collection.find_one({"name": "Test"})
#
#     # remove test doc so they dont pile up
#     client.drop_database('test_database')
#
#     logging.basicConfig(level=logging.INFO)
#     logging.info("MongoDB Client Operable")
#
#     pass

# def featureset_mongodb(set_id: str,
#                        endpoin: str,
#                        features_df: pd.DataFrame,
#                        date_col: str,
#                        target_col: str,
#                        selector: str=None):
#     """
#     store featureset information
#     set_id is optional since th euser can either create featuresets and store them for
#     later use, or have them stored by an automated selection system
#     selector is intended as the system used to select features if present
#     :param endpoint:
#     :return:
#     """
#
#     if not selector:
#         selector = 'manual'
#
#     mongodb_client = get_mongo_connection(endpoint=endpoint)
#     db = mongodb_client[MODELLING_DB_NAME]
#
#     # For a single-index DataFrame, push to the named collection
#     collection = db[selector]
#
#     feature_lbls = features_df.columns.tolist()
#     upper = features_df[date_col].max()
#     lower = features_df[date_col].min()
#
#     record = {'features': feature_lbls,
#               'upper_training_bound': upper,
#               'lower_training_bound': lower,
#               'target': target_col}
#     collection.insert(record)  # Insert into collection
#
#     logging.info(f"Loaded featureset into MongoDB:{MODELLING_DB_NAME}:{selector} from: {selector}")


# TODO write some actual tests at some point, put them somewhere in another file such
# TODO that imports arent fucked
# def test_mongo():
#     """
#     test single and multi index dataframe loading and extracting for pandas pipelines
#     :return:
#     """
#
#     # test local con
#     # client = get_mongo_connection(endpoint='local', endpoint_addr=None)
#     # single_collction_extract_load(client=client)
#
#     # test ngrok con specified
#     client = get_mongo_connection(endpoint='ngrok')
#     # dt = mongodb_latestdatetime(client=client, db_name='coinma')
#     single_collction_extract_load(client=client)
#
#     return
#
#
# def single_collction_extract_load(client):
#
#     # test single dataframe capability
#     fake_singledf = create_single_dataframe()
#
#     db_name = 'test_database'
#     collection_name = 'test_collection'
#
#     # load pandas dataframe to mongo
#     pandf_mongodb(
#         client=client,
#         df=fake_singledf,
#         db_name=db_name,
#         collection_name=collection_name
#     )
#
#     # extract pandas dataframe from mongo
#     df = mongodb_pandf(
#         client=client,
#         db_name=db_name,
#         collection_name=collection_name
#     )
#
#     client.drop_database(db_name)  # drop the test db when finished
#
#     return


if __name__ == '__main__':
    test_mongo()
