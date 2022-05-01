import json
import logging
from multiprocessing.pool import Pool

from util.TweepyConnector import TweepyConnector

from util.util import create_dir, Config, multiprocess_data_collection

from util.util import DataCollector
from util import Constants

from util.util import equal_chunks

FIELDS = [ "created_at"]

class Tweet:

    def __init__(self, tweet_id, news_id, news_source, label):
        self.tweet_id = tweet_id
        self.news_id = news_id
        self.news_source = news_source
        self.label = label


def dump_tweet_information(tweet_chunk: list, config: Config, tweepy_connector: TweepyConnector):
    """Collect info and dump info of tweet chunk containing atmost 100 tweets"""

    tweet_list = []
    for tweet in tweet_chunk:
        tweet_list.append(tweet.tweet_id)

    try:
        client = tweepy_connector.get_tweepy_connection(Constants.GET_TWEET)
        tweet_objects = client.get_tweets(ids=tweet_list, tweet_fields=FIELDS)


        tweets = {tweet.tweet_id: tweet for tweet in tweet_chunk}
        tweet_chunk = []
        for tweet in tweet_objects.data:
            tweet_chunk.append(tweets[tweet.id])

        for tweet_object, tweet in zip(tweet_objects.data, tweet_chunk):
            if tweet_object:
                dump_dir = "{}/{}/{}/{}".format(config.dump_location, tweet.news_source, tweet.label, tweet.news_id)
                tweet_dir = "{}/tweets".format(dump_dir)
                create_dir(dump_dir)
                create_dir(tweet_dir)
                json.dump(dict(tweet_object), open("{}/{}.json".format(tweet_dir, tweet.tweet_id), "w"), default=str)

    except Exception as ex:
        logging.exception("exception in collecting tweet objects")
    return None


def collect_tweets(news_list, news_source, label, config: Config):
    create_dir(config.dump_location)
    create_dir("{}/{}".format(config.dump_location, news_source))
    create_dir("{}/{}/{}".format(config.dump_location, news_source, label))

    save_dir = "{}/{}/{}".format(config.dump_location, news_source, label)

    tweet_id_list = []

    for news in news_list:
        for tweet_id in news.tweet_ids:
            tweet_id_list.append(Tweet(tweet_id, news.news_id, news_source, label))

    tweet_chunks = equal_chunks(tweet_id_list, 10)
    multiprocess_data_collection(dump_tweet_information, tweet_chunks, (config, config.tweepy_connector), config)


class TweetCollector(DataCollector):

    def __init__(self, config):
        super(TweetCollector, self).__init__(config)

    def collect_data(self, choices):
        for choice in choices:
            news_list = self.load_news_file(choice)
            collect_tweets(news_list, choice["news_source"], choice["label"], self.config)
