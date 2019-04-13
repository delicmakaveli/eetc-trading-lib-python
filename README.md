EETC Algo Trading
=====================

Description
-----------
Algorithmic Trading Python Library by EETC.

This library simplifies writing and running algorithmic trading bots.
It integrates EETC services for placing orders and receiving live data, allowing the developer
to focus solely on implementing the trading algorithm.


How it works
------------
This library connects to [EETC Data Feed](https://github.com/delicmakaveli/eetc-data-feed) and receives live data via [ZeroMQ](http://zeromq.org/).
To place orders for Stocks, Options, Crypto, etc. this library communicates with [EETC Order Manager](https://github.com/delicmakaveli/eetc-order-manager-crypto) via ZeroMQ.

Example [code](https://github.com/delicmakaveli/eetc-trading-lib-python/blob/master/examples/simple.py):
```python
from eetc_algo_trading import EETCTradingBot


def algorithm(bot_instance, topic=None, manual_trigger_details=None):
    bot_instance.algorithm_lock = True  # kinda "obtain" lock
    if topic:
        print("Executing Strategy for Topic: {}".format(topic))
        # whatever logic
    elif manual_trigger_details:
        print("Executing Strategy Manually...")
        print("Request data:", manual_trigger_details)
        # whatever logic
    else:
        print("Executing Strategy...")
        # whatever logic

    bot_instance.algorithm_lock = False  # kinda "release" lock


bot = EETCTradingBot(
    algorithm=algorithm,
    eetc_api_key="rAnDoMaPiKeyProvidedbyEETC",
    data_feed_topics=["candles:BTC/USD:1m"],
    trigger_on_topics=["candles:BTC/USD:1m"],
    allow_remote_triggering=False,
)

bot.start()
```

The only thing a developer needs to do is write the "algorithm" function and pass it to the EETCTradingBot during initialization.

### Authentication
To be able to receive data or execute trades, an API key is needed, which will be provided to you by EETC.
Although this library is open-sourced, nobody who isn't a client of EETC will be able to use the services that this library uses without the API key.

To become a client and obtain your API key, please contact us at: [eastempiretradingcompany2019@gmail.com](eastempiretradingcompany2019@gmail.com)

### Order management
It is entirely up to the developer to implement their own order management logic.
[EETC Order Manager](https://github.com/delicmakaveli/eetc-order-manager-crypto) provides various APIs where clients can get order information and receive real-time updates.

The most common tactic is to write a helper function for managing orders which will be executed within the algorithm function.

This approach may not be the most user-friendly, but it was chosen because it gives the developer absolute freedom for writing their strategy, which includes order management.

### Manual execution via ZeroMQ
Strategies can be triggered either manually via ZeroMQ by sending a request via REQ-REP sockets.
What information you put inside this request and how you process it is entirely up to you.
One simple use case for this might be when one algorithm is not sure about a trading decision, it can call
another algorithm which may be able to do that.

### Event-based execution
Strategies can also be triggered whenever a certain kind of data signal comes in (topic).
For example on each "candles:BTC/USD:1m" signal, execute the strategy.

### Scheduled execution

Coming soon...


System Requirements
-------------------
- Python 3.6 (should also work with other versions of Pythhon 3)
- TA-Lib C library installed (https://mrjbq7.github.io/ta-lib/install.html)

Installation
------------

- PyPi repository: https://pypi.org/project/eetc-algo-trading-lib/
- Run command: `pip install eetc-algo-trading-lib`

Licence
-------
This project is licensed under [GNU Public License](https://github.com/delicmakaveli/eetc-trading-lib-python/blob/master/LICENSE).
