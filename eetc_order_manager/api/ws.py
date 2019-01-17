import json
import threading
from websocket import WebSocketApp

from config import (
    EETC_ORDER_MANAGER_CLIENT_ID,
    EETC_ORDER_MANAGER_BASE_URL_WS,
)


def on_message(ws, message):
    print(json.loads(message))


def on_error(ws, error):
    error_str = 'ERROR: {}'.format(error)
    print(error_str)


def on_close(ws):
    print('Websocket client closed.')


def on_open(ws):
    print('Websocket client open.')
    # TODO: We have a proper STOMP message, but we can't subscribe.Only works in
    # JavaScript and there's no good way to use STOMP over WebSocket in Python.
    sub = "SUBSCRIBE\nid:3\ndestination:{}\n\n\x00".format(
        "/user/{}/queue/order".format(EETC_ORDER_MANAGER_CLIENT_ID),
    )
    ws.send(sub)


def get_order_ws_subscription_url() -> str:
    return EETC_ORDER_MANAGER_BASE_URL_WS


def get_websocket_client() -> WebSocketApp:
    return WebSocketApp(
        get_order_ws_subscription_url(),
        on_open=on_open,
        on_close=on_close,
        on_message=on_message,
        on_error=on_error,
    )


def ws_thread_routine(ws_app: WebSocketApp):
    ws_app.run_forever()


def subscribe_to_order_updates():
    ws_app = get_websocket_client()
    ws_thread = threading.Thread(target=ws_thread_routine, args=(ws_app, ))
    ws_thread.setDaemon(True)
    ws_thread.start()
