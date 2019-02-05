import json
import threading
from collections import deque
from datetime import datetime
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
        self.remote_trigger_thread = RemoteTriggerThread(daemon=True)

        self.data_feed_thread.bot_instance = self
        self.order_manager_thread.bot_instance = self
        self.remote_trigger_thread.bot_instance = self

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
        if self.allow_remote_triggering:
            self.remote_trigger_thread.start()

        while True:
            sleep(3600)  # just some bullshit so we don't kill the CPU :)

    def authenticate(self):
        r = self.order_manager_client.authenticate()

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
    zmq_req_socket = None

    bot_instance = None

    def run(self):
        """
        TODO
        :return:
        """
        self.zmq_context = zmq.Context()
        self.zmq_sub_socket = self.zmq_context.socket(zmq.SUB)
        self.zmq_sub_socket.connect(self.bot_instance.eetc_data_feed_zmq_sub_url)
        self.zmq_req_socket = self.zmq_context.socket(zmq.REQ)
        self.zmq_req_socket.connect(self.bot_instance.eetc_data_feed_zmq_req_url)

        self.get_data_snapshot()

        for topic in self.bot_instance.data_feed_topics:
            self.zmq_sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic)

        while True:
            multipart_msg = self.zmq_sub_socket.recv_multipart()
            topic = multipart_msg[0].decode()
            data = json.loads(multipart_msg[1].decode())

            if topic.startswith('candles'):
                process_candle_data(self.bot_instance, topic, data)
            elif topic.startswith('book'):
                process_order_book_data(self.bot_instance, topic, data)
            elif topic.startswith('trades'):
                process_trade_data(self.bot_instance, topic, data)

            # extract these to variables just for readability
            trigger_topics = self.bot_instance.trigger_on_topics

            if not self.bot_instance.algorithm_lock and topic in trigger_topics:
                algorithm_thread = threading.Thread(
                    target=self.bot_instance.algorithm,
                    args=(self.bot_instance, topic),
                    daemon=True,
                )
                algorithm_thread.start()

    def get_data_snapshot(self):
        """
        TODO
        :return:
        """
        for topic in self.bot_instance.data_feed_topics:
            self.zmq_req_socket.send(topic.encode())
            response = self.zmq_req_socket.recv_multipart()

            data = json.loads(response[1].decode())

            if topic.startswith('candles'):
                process_candle_data(self.bot_instance, topic, data)
            elif topic.startswith('book'):
                process_order_book_data(self.bot_instance, topic, data)
            elif topic.startswith('trades'):
                process_trade_data(self.bot_instance, topic, data)


def algorithm_manual_trigger_routine(bot_instance, manual_trigger_details):
    """
    TODO
    :param bot_instance:
    :param manual_trigger_details:
    :return:
    """
    while True:
        # keep trying until the algorithm lock becomes free, then trigger
        if not bot_instance.algorithm_lock:
            bot_instance.algorithm(
                bot_instance, manual_trigger_details=manual_trigger_details,
            )
            break


class RemoteTriggerThread(threading.Thread):
    """
    TODO
    """
    zmq_context: zmq.Context = None
    zmq_rep_socket: zmq.Socket = None

    bot_instance = None

    def run(self):
        """
        TODO
        :return:
        """
        self.zmq_context = zmq.Context()
        self.zmq_rep_socket = self.zmq_context.socket(zmq.REP)
        self.zmq_rep_socket.bind("tcp://*:21913")

        while True:
            try:
                request = json.loads(self.zmq_rep_socket.recv().decode())

                # Trigger algorithm in a separate worker thread
                worker_thread = threading.Thread(
                    target=algorithm_manual_trigger_routine,
                    args=(self.bot_instance, request, ),
                    daemon=True,
                )
                worker_thread.start()

                reply = {"Message": "Algorithm triggered successfully."}
            except Exception as e:
                reply = {"Message": "Something went wrong.", "Error": str(e)}

            self.zmq_rep_socket.send(json.dumps(reply).encode())


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


# TODO add type hints for all these functions

def timestamp_to_datetime_str(timestamp):
    """
    TODO
    :param timestamp:
    :return:
    """
    return datetime.fromtimestamp(
        int(timestamp[:10]),
    ).strftime('%Y-%m-%d %H:%M:%S')


def is_date_bigger_than(date_str: str, than_str: str) -> bool:
    """
    Check if date is "bigger" (later) than the other
    :param date_str: datetime string of date we wish to compare
    :param than_str: datetime string of date we wish to compare to
    :return: True/False
    """
    date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    than = datetime.strptime(than_str, '%Y-%m-%d %H:%M:%S')
    return date > than


# TODO move these inside EETCTradingBot class, it's cleaner

def process_order_book_data(bot_instance=None, topic=None, latest_data=None):
    """
    TODO
    """
    if not latest_data or not topic or not bot_instance:
        return

    if len(latest_data) > 1:
        # storing in dict for performance
        order_book = {}
        for price_lvl_data in latest_data:
            order_book[price_lvl_data['price']] = price_lvl_data

        bot_instance.data[topic] = order_book
    else:
        if topic in bot_instance.data:
            # https://docs.bitfinex.com/v2/reference#ws-public-order-books
            if latest_data[0]['count'] == 0:
                bot_instance.data[topic].pop(latest_data[0]['price'], None)
            else:
                bot_instance.data[topic].update(
                    {latest_data[0]['price']: latest_data[0]},
                )


def process_trade_data(bot_instance=None, topic=None, latest_data=None):
    """
    TODO
    """
    if not latest_data or not topic or not bot_instance:
        return

    if len(latest_data) > 1:
        # storing in deque for performance
        bot_instance.data[topic] = deque(latest_data, maxlen=len(latest_data))
    else:
        if topic in bot_instance.data:
            bot_instance.data[topic].append(latest_data[0])


def process_candle_data(bot_instance=None, topic=None, latest_data=None):
    """
    TODO
    """
    if not latest_data or not topic or not bot_instance:
        return

    if len(latest_data) > 1:
        bot_instance.data[topic] = latest_data
    else:
        if topic in bot_instance.data:
            latest_data = latest_data[-1]
            latest_time = timestamp_to_datetime_str(str(latest_data['time']))
            last_time = timestamp_to_datetime_str(str(
                bot_instance.data[topic][-1]['time'],
            ))

            if is_date_bigger_than(latest_time, last_time):
                # add new candle
                bot_instance.data[topic].append(latest_data)
                bot_instance.data.pop(0, None)
            elif last_time == latest_time:
                # update the latest one
                bot_instance.data[topic][-1].update(latest_data)
