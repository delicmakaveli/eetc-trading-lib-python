import json
import requests
from uuid import uuid4

from config import (
    EETC_ORDER_MANAGER_BASE_URL_HTTP,
    EETC_ORDER_MANAGER_API_KEY_HEADER,
    EETC_ORDER_MANAGER_API_KEY,
    EETC_ORDER_MANAGER_CLIENT_ID,
)


def process_reponse(resp: requests.Response) -> dict:
    data = resp.json()

    assert resp.status_code == 200, "Bad Response: {}:{}".format(
        resp.status_code, data,
    )

    return data


def get_order(order_id: int) -> dict:
    """
    Sends GET request to REST API to get info about a specific order.
    :param order_id:
    :return:
    """
    resp = requests.get(
        '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/order/get'),
        params={'id': order_id},
        headers={EETC_ORDER_MANAGER_API_KEY_HEADER: EETC_ORDER_MANAGER_API_KEY},
    )
    data = process_reponse(resp)

    # convert "extra" from str to dict for convenience
    data['extra'] = json.loads(data['extra'])

    return data


def get_client_orders(client_id: int = EETC_ORDER_MANAGER_CLIENT_ID) -> dict:
    """
    Sends GET request to REST API to get info about all orders for this client.
    :param client_id:
    :return:
    """
    resp = requests.get(
        '{}{}'.format(EETC_ORDER_MANAGER_BASE_URL_HTTP, '/api/order/client'),
        params={'id': client_id},
        headers={EETC_ORDER_MANAGER_API_KEY_HEADER: EETC_ORDER_MANAGER_API_KEY},
    )
    data = process_reponse(resp)

    # convert "extra" from str to dict for convenience
    for order in data:
        order['extra'] = json.loads(order['extra'])

    return data


def place_order(asset: int, amount: float, action: str, type: int, broker: str,
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
            EETC_ORDER_MANAGER_API_KEY_HEADER: EETC_ORDER_MANAGER_API_KEY,
        },
    )
    data = process_reponse(resp)

    # convert "extra" from str to dict for convenience
    data['extra'] = json.loads(data['extra'])

    return data
