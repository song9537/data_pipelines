from dn757657_data_endpoints.mongoDB import get_mongo_connection, mongodb_latestdatetime
from dn757657_crypto_num_sources.bitfin import bitfinbatch_pandf

mongodb_conn = get_mongo_connection(endpoint='local')

start = mongodb_latestdatetime(
    db_name='bitfinex',
    collection_name='btcusd',
    mongodb_client=mongodb_conn,
    time_col='bitfinex_btcusd_time'
)

df = bitfinbatch_pandf(
    pair_code='btcusd',
    start=start
)
print()
