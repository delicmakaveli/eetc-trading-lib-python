import json
import threading
from time import sleep
from typing import Callable
from uuid import uuid4

import requests
import zmq


EETC_ORDER_MANAGER_API_KEY_HEADER = 'EETC-API-KEY'

EETC_ORDER_MANAGER_BASE_URL_HTTP = 'http://localhost:8080'
EETC_ORDER_MANAGER_BASE_URL_HTTPS = 'https://localhost:8443'

EETC_ORDER_MANAGER_BASE_URL_WS = 'ws://localhost:8080/eetc-websocket'
EETC_ORDER_MANAGER_BASE_URL_WSS = 'wss://localhost:8443/eetc-websocket'


class EETCTradingBot:
    """
    TODO
    """
    def __init__(self, algorithm: Callable, eetc_api_key: str,
                 data_feed_topics: list, trigger_on_topics: list,
                 allow_remote_triggering: bool = False,
                 ):
        """
        TODO
        :param algorithm:
        :param eetc_api_key:
        :param data_feed_topics:
        :param trigger_on_topics:
        :param allow_remote_triggering:
        """
        self.algorithm = algorithm
        self.eetc_api_key = eetc_api_key
        self.data_feed_topics = data_feed_topics
        self.trigger_on_topics = trigger_on_topics
        # TODO implement manual remote triggering logic
        self.allow_remote_triggering = allow_remote_triggering

        self.order_manager_client = EETCOrderManagerRESTClient(eetc_api_key)

        self.eetc_data_feed_zmq_sub_url = None
        self.eetc_data_feed_zmq_req_url = None
        self.eetc_order_manager_zmq_sub_url = None

        self.data = {}

        # Ensure that the algorithm will always read the most up-to-date data
        self.placed_orders_lock = threading.Lock()
        self.placed_orders = {}

        # this is to ensure the algorithms runs only once at a time
        # we don't need a real lock here, flag is ok
        self.algorithm_lock = False

        self.order_manager_thread = EETCOrderManagerThread(daemon=True)
        self.data_feed_thread = EETCDataFeedThread(daemon=True)

        self.data_feed_thread.bot_instance = self
        self.order_manager_thread.bot_instance = self

    def start(self):
        """
        TODO
        :return:
        """
        # Authenticate via API Key to get ZeroMQ URLs for EETC Data Feed
        self.authenticate()

        self.order_manager_thread.start()
        sleep(1)  # idk why the fuck do I have this...
        self.data_feed_thread.start()

        while True:
            sleep(3600)  # just some bullshit so we don't kill the CPU :)

    def authenticate(self):
        r = self.order_manager_client.authenticate()

        print(r)

        self.eetc_data_feed_zmq_sub_url = r.get("eetc_data_feed_zmq_sub_url")
        self.eetc_data_feed_zmq_req_url = r.get("eetc_data_feed_zmq_req_url")
        self.eetc_order_manager_zmq_sub_url = r.get("eetc_order_manager_zmq_sub_url")

        assert self.eetc_data_feed_zmq_sub_url, "Authentication failed"
        assert self.eetc_data_feed_zmq_req_url, "Authentication faield"
        assert self.eetc_order_manager_zmq_sub_url, "Authentication failed"


class EETCOrderManagerThread(threading.Thread):
    """
    TODO
    """
    placed_orders = {}
    zmq_context = None
    zmq_sub_socket = None

    bot_instance = None

    def run(self):
        """
        TODO
        :return:
        """
        self.zmq_context = zmq.Context()
        self.zmq_sub_socket = self.zmq_context.socket(zmq.SUB)
        self.zmq_sub_socket.connect(
            self.bot_instance.eetc_order_manager_zmq_sub_url,
        )
        self.zmq_sub_socket.setsockopt_string(
            zmq.SUBSCRIBE,
            "orders:{}".format(self.bot_instance.eetc_api_key),
        )

        while True:
            multipart_msg = self.zmq_sub_socket.recv_multipart()
            order_data = json.loads(multipart_msg[1].decode())

            try:
                self.bot_instance.placed_orders_lock.aquire()

                if order_data["id"] in self.placed_orders:
                    self.bot_instance.placed_orders["id"].update(order_data)
                else:
                    self.bot_instance.placed_orders["id"] = order_data

                print("Order update:", order_data)
            finally:
                self.bot_instance.placed_orders_lock.release()


class EETCDataFeedThread(threading.Thread):
    """
    TODO
    """
    zmq_context = None
    zmq_sub_socket = None

    bot_instance = None

    def run(self):
        """
        TODO
        :return:
        """
        self.zmq_context = zmq.Context()
        self.zmq_sub_socket = self.zmq_context.socket(zmq.SUB)
        self.zmq_sub_socket.connect(self.bot_instance.eetc_data_feed_zmq_sub_url)

        # TODO get initial data for all desired topics via ZeroMQ REQ-REP

        for topic in self.bot_instance.data_feed_topics:
            self.zmq_sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic)

        while True:
            multipart_msg = self.zmq_sub_socket.recv_multipart()
            topic = multipart_msg[0].decode()
            data = json.loads(multipart_msg[1].decode())

            # TODO implement data maintenance logic for bot_instance.data
            self.bot_instance.data[topic] = data

            # extract these to variables just for readability
            algorithm_lock = self.bot_instance.algorithm_lock
            trigger_on_topics = self.bot_instance.trigger_on_topics

            if not algorithm_lock and topic in trigger_on_topics:
                algorithm_thread = threading.Thread(
                    target=self.bot_instance.algorithm,
                    args=(self.bot_instance, topic),
                    daemon=True,
                )
                algorithm_thread.start()


class EETCOrderManagerRESTClient:
    """
    TODO
    """
    def __init__(self, eetc_api_key: str):
        """
        TODO
        :param eetc_api_key:
        """
        self.eetc_api_key = eetc_api_key

    def process_reponse(self, resp: requests.Response) -> dict:
        data = resp.json()

        assert resp.status_code == 200, "Bad Response: {}:{}".format(
            resp.status_code, data,
        )

        return data

    def get_order(self, order_id: int) -> dict:
        """
        Sends GET request to REST API to get info about a specific order.
        :param order_id:
        :return:
        """
        resp = requests.get(
            '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/order/get'),
            params={'id': order_id},
            headers={EETC_ORDER_MANAGER_API_KEY_HEADER: self.eetc_api_key},
        )
        data = self.process_reponse(resp)

        # convert "extra" from str to dict for convenience
        data['extra'] = json.loads(data['extra'])

        return data

    def get_client_orders(self) -> dict:
        """
        Sends GET request to REST API to get info about all orders for this client.
        :param client_id:
        :return:
        """
        resp = requests.get(
            '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/order/client'),
            headers={EETC_ORDER_MANAGER_API_KEY_HEADER: self.eetc_api_key},
        )
        data = self.process_reponse(resp)

        # convert "extra" from str to dict for convenience
        for order in data:
            order['extra'] = json.loads(order['extra'])

        return data

    def place_order(self, asset: int, amount: float, action: str, type: int,
                    broker: str,
                    limit_price: float = None, stop_price: float = None,
                    extra: dict = None,
                    ) -> dict:
        """
        Sends POST request to REST API to place BUY/SELL Orders.
        :param asset:
        :param amount:
        :param action:
        :param type:
        :param broker:
        :param limit_price:
        :param stop_price:
        :param extra:
        :return:
        """
        assert amount > 0, "'amount' must be greater than 0"
        assert action.upper() in ["BUY", "SELL"], "'action' must be 'BUY' or 'SELL'"

        if not extra:
            extra = {}

        payload = {
            'asset': {'id': asset},
            'amount': amount,
            'action': action.upper(),
            'type': type,
            'broker': broker,
            'uuid': str(uuid4()),
            'extra': str(extra).replace("'", "\n"),
        }
        if limit_price:
            payload['limitPrice'] = limit_price
        if stop_price:
            payload['stopPrice'] = stop_price

        resp = requests.post(
            '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/order/create'),
            json=payload,
            headers={
                'Content-type': 'application/json',
                EETC_ORDER_MANAGER_API_KEY_HEADER: self.eetc_api_key,
            },
        )
        data = self.process_reponse(resp)

        # convert "extra" from str to dict for convenience
        data['extra'] = json.loads(data['extra'])

        return data

    def get_assets(self) -> dict:
        """
        Sends GET request to REST API to get info about all Assets.
        :return:
        """
        resp = requests.get(
            '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/asset/list'),
            headers={EETC_ORDER_MANAGER_API_KEY_HEADER: self.eetc_api_key},
        )
        data = self.process_reponse(resp)

        # convert "extra" from str to dict for convenience
        for asset in data:
            asset['extra'] = json.loads(asset['extra'])

        return data

    def get_asset(self, asset_id: int) -> dict:
        """
        Sends GET request to REST API to get info about a specific Asset.
        :param asset_id:
        :return:
        """
        resp = requests.get(
            '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/asset/get'),
            params={'id': asset_id},
            headers={EETC_ORDER_MANAGER_API_KEY_HEADER: self.eetc_api_key},
        )
        data = self.process_reponse(resp)

        # convert "extra" from str to dict for convenience
        data['extra'] = json.loads(data['extra'])

        return data

    def authenticate(self) -> dict:
        """
        Authenticate client via REST API and get client-sensitive data.
        :return:
        """
        resp = requests.get(
            '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/client/auth'),
            headers={EETC_ORDER_MANAGER_API_KEY_HEADER: self.eetc_api_key},
        )
        data = self.process_reponse(resp)

        return data
