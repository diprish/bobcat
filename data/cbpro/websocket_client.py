# cbpro/WebsocketClient.py
# original author: Daniel Paquin
# mongo "support" added by Drew Rice
#
#
# Template object to receive messages from the Coinbase Websocket Feed

from __future__ import print_function
import json
import base64
import hmac
import hashlib
import time
from threading import Thread
from websocket import create_connection, WebSocketConnectionClosedException
from pymongo import MongoClient
import pymongo
from data.cbpro.cbpro_auth import get_auth_headers
import logging


class WebsocketClient(object):
    def __init__(
            self,
            url="wss://ws-feed.pro.coinbase.com",
            products=None,
            message_type="subscribe",
            auth=False,
            api_key="",
            api_secret="",
            api_passphrase="",
            db_client="",
            # Make channels a required keyword-only argument; see pep3102
            *,
            # Channel options: ['ticker', 'user', 'matches', 'level2', 'full']
            channels):
        self.url = url
        self.products = products
        self.channels = channels
        self.type = message_type
        self.stop = True
        self.error = None
        self.ws = None
        self.thread = None
        self.auth = auth
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.db_client = db_client

    def start(self):
        def _go():
            self._connect()
            self._listen()
            self._disconnect()

        self.stop = False
        self.on_open()
        self.thread = Thread(target=_go)
        self.keepalive = Thread(target=self._keepalive)
        self.thread.start()

    def _connect(self):
        if self.products is None:
            raise Exception("Provide the products pair list")
        elif not isinstance(self.products, list):
            self.products = [self.products]
        if self.url[-1] == "/":
            self.url = self.url[:-1]

        if self.channels is None:
            self.channels = [{"name": "ticker", "product_ids": [product_id for product_id in self.products]}]
            sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}
        else:
            sub_params = {'type': 'subscribe', 'product_ids': self.products, 'channels': self.channels}

        if self.auth:
            timestamp = str(time.time())
            message = timestamp + 'GET' + '/users/self/verify'
            auth_headers = get_auth_headers(timestamp, message, self.api_key, self.api_secret, self.api_passphrase)
            sub_params['signature'] = auth_headers['CB-ACCESS-SIGN']
            sub_params['key'] = auth_headers['CB-ACCESS-KEY']
            sub_params['passphrase'] = auth_headers['CB-ACCESS-PASSPHRASE']
            sub_params['timestamp'] = auth_headers['CB-ACCESS-TIMESTAMP']

        self.ws = create_connection(self.url)

        self.ws.send(json.dumps(sub_params))

    def _keepalive(self, interval=30):
        while self.ws.connected:
            self.ws.ping("keepalive")
            time.sleep(interval)

    def _listen(self):
        self.keepalive.start()
        while not self.stop:
            try:
                data = self.ws.recv()
                msg = json.loads(data)
            except ValueError as e:
                self.on_error(e)
            except Exception as e:
                self.on_error(e)
            else:
                self.on_message(msg)

    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass
        finally:
            self.keepalive.join()

        self.on_close()

    def close(self):
        self.stop = True   # will only disconnect after next msg recv
        self._disconnect() # force disconnect so threads can join
        self.thread.join()

    def on_open(self):
        logging.info("-- Subscribed! --\n")
        self.message_count = 0
        self.insert_count = 0
        logging.info("Let's count the messages!")

    def on_close(self):
        logging.info("\n-- Socket Closed --")
        logging.info('Message Count {}. Insert Count {}'.format(self.message_count, self.insert_count))

    def on_message(self, msg):
        logging.info(msg)
        
        # Read ticker message
        if (msg['type']=='ticker'):
            self.message_count += 1
            self.update_data(ticker_msg=msg)
        
    def update_data(self, ticker_msg):
        _product_name = ticker_msg['product_id'].replace('-','_')
        _product_collection = self.db_client[_product_name]
        
        if _product_name not in self.db_client.list_collection_names():
            _product_collection.create_index("sequence", unique=True)
        else:
            # Collection exists
            try:
                _id = _product_collection.insert_one(ticker_msg).inserted_id
                self.insert_count += 1
            except pymongo.errors.DuplicateKeyError:
                logging.info("Duplicate Key Error: {}".format(ticker_msg))

    def on_error(self, e, data=None):
        self.error = e
        self.stop = True
        logging.error('{} - data: {}'.format(e, data))
