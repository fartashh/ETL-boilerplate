import sys

sys.path.append('/home/fartash/Dev/ETL-boilerplate')
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import time
from distrbute_tasks.tasks import *


class TwitterExtractor():
    def __init__(self):
        self.consumer_key = 'kUGqrPUEtaDmPEMuw3z1A'
        self.consumer_secret = 't95cSJ5rdY7HDu81BJ393xTNuS68JbcMowcWnppr0'
        self.access_token = '19952367-gD96uxT6CRkHVY5YZB4PgsY7tA3ISNhGBS42bzANg'
        self.access_secret = '1aspN8mbHy0v8hS4H4lWVJ4AiWLVCxhHxeej0pPGmE'

        self.auth = OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_token, self.access_secret)
        self.status = []

    def process(self, query):
        stream = Stream(self.auth, self.TweetListener(self), secure=True, )
        stream.filter(track=[query])

    class TweetListener(StreamListener):

        def __init__(self, parent):
            StreamListener.__init__(self)
            self.parent = parent

        def on_status(self, status):
            try:
                save_into_db.delay('tweet', json.dumps(status._json).replace("'", "''"), str(status.created_at))
            except Exception as ex:
                print (ex)
                return False


if __name__ == "__main__":
    twitter_extractor = TwitterExtractor()
    twitter_extractor.process(u"Trump")
