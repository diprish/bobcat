from data.cbpro.websocket_client import WebsocketClient
from data.products import Products
import pprint

"""
Class Objectives
    - Able to get Products details data and insert it into the mongodb
    - Able to refresh Products details data and update it into the mongodb
    - Able to share the list of products details based on a query
"""

class ProductDetails:
    def __init__(self, db_client) -> None:
        self.db_client = db_client

    def load_products(self):
        # Get list of products
        products = Products.get_products(
            self.db_client,
            query={
                "quote_currency": "USD",
                "trading_disabled": False,
                "status": "online",
            },
            projection={"id": True, "_id": False},
        )
        product_list = []
        for p in products:
            product_list.append(p['id'])

        # Pass the product list to the WebService
        wsClient = WebsocketClient(channels=['ticker'], products=product_list, db_client=self.db_client)
        wsClient.start()