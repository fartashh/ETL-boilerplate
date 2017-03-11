from gender_detector import GenderDetector
from geolocation.main import GoogleMaps
from textblob import TextBlob
from nltk.corpus import stopwords, wordnet
from nltk import bigrams, trigrams
from services import db_connection_handler as db_handler
from datetime import datetime, timedelta
from dateutil import parser
from config import app_settings
import re
import string
import json


class TwitterTransformer():
    def __init__(self):
        self.gender_detector = GenderDetector()
        self.googlemaps_api = GoogleMaps(api_key=app_settings.SERVICES_CREDENTIALS['google_api_key'])

    def process(self, ds, **kwargs):
        raw_records = self.__fetch_tweets()
        print "{} new tweets have been analyzed".format(len(raw_records))
        conn_db_lake = db_handler.get_connection('data_lake')
        cur_db_lake = conn_db_lake.cursor()

        for record in raw_records:
            tweet = record[2]
            clean_tweet = self.__tweet_cleaner(tweet['text'])
            print clean_tweet
            polarity, sentiment = self.__get_sentiment(clean_tweet)
            coordinates = self.__get_go_points(tweet['user']['location'])
            gender = self.__guess_gender(tweet['user']['name'].split()[0])
            tweet_tokens = self.__tokenizer(clean_tweet)
            processed_tweet = {
                "author": tweet["user"]["screen_name"],
                "tweet_geo": tweet['geo'],
                "tweet_lang": tweet['lang'],
                "tweet_place": tweet['place'],
                "user_description": tweet['user']['description'],
                "user_followers_count": tweet['user']['followers_count'],
                "user_friends_count": tweet['user']['friends_count'],
                "user_lang": tweet['user']['lang'],
                "user_name": tweet['user']['name'],
                "user_location_name": tweet['user']['location'],
                "user_location_coordinate": {"lat": coordinates[0], "lon": coordinates[1]} if coordinates else None,
                "user_status_count": tweet['user']['statuses_count'],
                "tweet_created_at": str(parser.parse(tweet['created_at'])),
                "user_created_at": str(parser.parse(tweet['user']['created_at'])),
                "tweet_tokens": tweet_tokens,
                'bigrams': ["_".join(x) for x in bigrams(tweet_tokens)],
                'trigrams': ["_".join(x) for x in trigrams(tweet_tokens)],
                "polarity": polarity,
                "sentiment": sentiment,
                "gender": gender,
            }

            try:
                update_query = """
                UPDATE records
                SET is_analyzed=TRUE
                WHERE id={};
                """.format(record[0])
                query = """INSERT INTO tweets (data, created_at) VALUES ('{}', '{}')""".format(
                    json.dumps(processed_tweet).replace("'", "''"), record[3])

                cur_db_lake.execute(query)
                cur_db_lake.execute(update_query)
                conn_db_lake.commit()
            except Exception as ex:
                conn_db_lake.rollback()
                raise ex

    def __fetch_tweets(self):
        try:
            conn_db_lake = db_handler.get_connection('data_lake')
            cur_db_lake = conn_db_lake.cursor()

            query = """
            SELECT * FROM records
            WHERE type='tweet' AND is_analyzed = false
            """

            cur_db_lake.execute(query)
            return cur_db_lake.fetchall()
        except Exception as ex:
            conn_db_lake.rollback()
            raise ex

    def __guess_gender(self, name):
        gender = None
        try:
            gender = self.gender_detector.guess(name)
            return gender
        except Exception as e:
            print('error in gender detector')

    def __get_go_points(self, address):
        if not address:
            return None
        coordinate = None
        try:
            res = self.googlemaps_api.search(address.strip(string.punctuation + ' ')).first()
            if res:
                coordinate = [res.lat, res.lng]
        except Exception as ex:
            print ("Err in geo location convertor")

        return coordinate

    def __tweet_cleaner(self, tweet):
        # Convert to lower case
        tweet = tweet.lower()
        # Convert www.* or https?://* to empty string
        tweet = re.sub('((www\.[\s]+)|(https?://[^\s]+))', '', tweet)
        # Convert @username to empty string
        tweet = re.sub('@[^\s]+', '', tweet)
        # Remove additional white spaces
        tweet = re.sub('[\s]+', ' ', tweet)
        # Replace #word with word
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
        # trim
        tweet = tweet.strip('\'"')

        return tweet

    def __get_sentiment(self, tweet):
        res = TextBlob(tweet)
        polarity = res.sentiment.polarity
        if polarity < 0:
            sentiment = 'negative'
        elif polarity == 0:
            sentiment = 'neutral'
        else:
            sentiment = 'positive'

        return (polarity, sentiment)

    def __tokenizer(self, tweet):
        tokens = []
        for word in tweet.split():
            if len(word) > 3 and word not in stopwords.words('english') and wordnet.synsets(word):
                tokens.append(word)
        return list(set(tokens))


if __name__ == "__main__":
    transformer = TwitterTransformer()
    transformer.process(datetime.utcnow(), **{'interval': timedelta(hours=5)})
