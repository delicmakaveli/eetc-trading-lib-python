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
