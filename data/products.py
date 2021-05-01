from pymongo import MongoClient
import pymongo
import logging
import data.cbpro as cbpro


"""
Class Objectives
    - Able to get Products data and insert it into the mongodb
    - Able to refresh Products data and update it into the mongodb
    - Able to share the list of products pairs based on a query
"""

class Products:
    def __init__(self, db_client) -> None:
        self.db_client = db_client
        self.cb_pro_public_client = cbpro.PublicClient()

    def get_products_from_cb(self):
        product_list = self.cb_pro_public_client.get_products()
        return product_list

    def get_products_from_cb_update_db(self):
        product_list = self.get_products_from_cb()
        self.update_data(product_list)
        logging.info('Product list refreshed')
        return product_list

    def refresh(self):
        self.db_client["products"].drop()
        logging.info("Product collections dropped")

    def update_data(self, product_list):
        for p in product_list:
            # Verify if the collection already exists
            p_collection = self.db_client["products"]
            if "products" not in self.db_client.list_collection_names():
                p_collection.create_index("id", unique=True)
            else:
                # Collection exists
                p_query = {"id": p['id']}
                p_collection.update_one(p_query, {"$set": p},upsert=True)
    
    @staticmethod
    def get_products(db_client, query, projection):
        p_cursor = db_client["products"].find(query, projection = projection)
        for p in p_cursor:
            yield p


if __name__ == "__main__":
    import sys
    import time
    import pprint

    db_client = MongoClient("mongodb://127.0.0.1:27017/")['cb_pro_db']
    products = Products(db_client)
    l = products.get_products_from_cb_update_db()
    pprint.pprint(l)
