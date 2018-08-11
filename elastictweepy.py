import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from elasticsearch import Elasticsearch

# import twitter keys and tokens
from config import *

# create instance of elasticsearch
es = Elasticsearch()


class TweetStreamListener(StreamListener):

    # on success
    def on_data(self, data):

        # decode json
        twitter_data = json.loads(data)

        # pass tweet into TextBlob
        tweet = TextBlob(twitter_data['text'])

        # output sentiment polarity
        print(tweet.sentiment.polarity)

        # determine if sentiment is positive, negative, or neutral
        if tweet.sentiment.polarity < 0:
            sentiment = "negative"
        elif tweet.sentiment.polarity == 0:
            sentiment = "neutral"
        else:
            sentiment = "positive"

        # output sentiment
        print (sentiment)

        # add text and sentiment info to elasticsearch
        es.index(index="twitterpull0811",
                 doc_type="test-type",
                 body={"author": twitter_data["user"]["screen_name"],
                       "date": twitter_data["created_at"],
                       "message": twitter_data["text"],
                       "location": twitter_data["coordinates"],
                       "description":twitter_data["user"]["description"],
                       "geo":twitter_data["geo"],
                       "user_created":twitter_data["user"]['created_at'],
                       "followers": twitter_data["user"]["followers_count"],
                       "id_str": twitter_data["id_str"],
                       "tweet_created": twitter_data["created_at"],
                       "followed_by": twitter_data["user"]["friends_count"],
                       "favorites": twitter_data["user"]["favourites_count"],
                       "geo_enabled": twitter_data["user"]["geo_enabled"],
                       "lang": twitter_data["user"]["lang"],
                       "status_count": twitter_data["user"]["statuses_count"],
                       "protected": twitter_data["user"]["protected"],
                       "polarity": tweet.sentiment.polarity,
                       "subjectivity": tweet.sentiment.subjectivity,
                       "sentiment": sentiment})
        return True


if __name__ == '__main__':

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for specific words and phrases
    stream.filter(track=["Clinton", "Illegal Immigrants", "#Q", "election"])
