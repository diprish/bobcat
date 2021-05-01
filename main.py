import sys
from pymongo import MongoClient
import logging
from data.products import Products
from data.product_details import ProductDetails

logging.basicConfig(filename='std.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s',level=logging.DEBUG)

db_client = MongoClient("mongodb+srv://bobcat:23G1Tk1KwgTOxtlU@cluster0.uailx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")['cb_pro_db']
db = db_client.test

# db_client = MongoClient("mongodb://127.0.0.1:27017/")['cb_pro_db']
products = Products(db_client)
l = products.get_products_from_cb_update_db()

# db_client = MongoClient("mongodb://127.0.0.1:27017/")["cb_pro_db"]
product_details = ProductDetails(db_client)
product_details.load_products()

# try:
#     while True:
#         print("\nMessageCount =", "%i \n" % wsClient.message_count)
#         time.sleep(1)
# except KeyboardInterrupt:
#     wsClient.close()


# if wsClient.error:
#     sys.exit(1)
# else:
#     sys.exit(0)