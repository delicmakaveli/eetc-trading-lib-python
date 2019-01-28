from eetc_lib import EETCTradingBot


def algorithm(bot_instance: EETCTradingBot,
              topic: str = None, manual_trigger_details: dict = None,
              ):
    if topic:
        print("Executing Strategy for Topic: {}".format(topic))
        # whatever logic
    elif manual_trigger_details:
        print("Executing Strategy Manually...")
        print(manual_trigger_details)
        # whatever logic
    else:
        print("Executing Strategy...")
        # whatever logic


bot = EETCTradingBot(
    algorithm=algorithm, eetc_api_key="rUyJjh6s9UKEFohZ6RVNclsqI6KtzLvP",
    data_feed_topics=[""], trigger_on_topics=["trades:BTC/USD"],
    allow_remote_triggering=False,
)

bot.start()
