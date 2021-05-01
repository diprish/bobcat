import cbpro
from pymongo import MongoClient
import pymongo


class ProductData():
    def __init__(self) -> None:
        pass

    @staticmethod
    def get_db_client(db_url='mongodb://localhost:27017/'):
        db_client = MongoClient(db_url)['coinbase_pro_db']
        return db_client


cbpro_client = cbpro.PublicClient()
# historylist = cbpro_client.get_product_historic_rates('AAVE-USD')
# print(historylist)

# Get all the ids from Product collection where the quote currency is USD
db_client = ProductData.get_db_client()
products_cursor = db_client.products.find(
    {"trading_disabled": False, "status": "online", "quote_currency": "USD"})

# products_list = []
# for rec in products_cursor:
#     products_list.append(rec["id"])

# For each client call the product history

# for p in products_list:
#     #Verify if the collection exists
#     p = p.replace('-','_')
#     p_collection = db_client[p]

#     if p not in db_client.list_collection_names():
#         p_collection.create_index('time',unique=True)
#         print('{} index created'.format(p))

#     p_data = cbpro_client.get_product_historic_rates(p.replace('_','-'))
#     print("Downloaded {} rows for pair {}".format(len(p_data), p))
#     # Insert data into the p_data
#     for r in p_data:
#         try:
#             p_collection.insert_one({'time':r[0], 'low':r[1],'high':r[2],'open':r[3],'close':r[4],'volume':r[5]})
#         except pymongo.errors.DuplicateKeyError:
#             continue

btc_collection = db_client['BTC_USD']
eth_collection = db_client['ETH-USD']
wsClient = cbpro.WebsocketClient(products=['BTC-USD','ETH-USD'],mongo_collection=btc_collection, should_print=False)
wsClient.start()
