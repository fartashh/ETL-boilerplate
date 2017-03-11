from services import db_connection_handler as db_handler
from elasticsearch import Elasticsearch, RequestsHttpConnection
from datetime import datetime, timedelta
from dateutil import parser


class TwitterLoader():
    def __init__(self):
        self.elastic = Elasticsearch(connection_class=RequestsHttpConnection)

    def load_into_elastic(self, ds, **kwargs):
        if not self.__is_elastic_index_available(index='tweets'):
            self.__create_elatic_index(index='tweets')

        docs = self.__fetch_tweets()
        print "{} tweets have been loaded into elatic".format(len(docs))

        conn_db_lake = db_handler.get_connection('data_lake')
        cur_db_lake = conn_db_lake.cursor()

        for doc in docs:
            tweet = doc[1]
            print tweet
            tweet['tweet_created_at'] = parser.parse(tweet['tweet_created_at'])
            tweet['user_created_at'] = parser.parse(tweet['user_created_at'])

            self.elastic.index('tweets', 'tweet', body=tweet)

            update_query = """
            UPDATE tweets
            SET is_analyzed=TRUE
            WHERE id={};
            """.format(doc[0])
            cur_db_lake.execute(update_query)
            conn_db_lake.commit()

    def __is_elastic_index_available(self, index):
        return self.elastic.indices.exists(index)

    def __create_elatic_index(self, index):
        self.elastic.indices.create(
            index=index,
            ignore=400
        )
        _mappings = {
            'tweet': {
                "properties": {
                    "user_location_coordinate": {"type": "geo_point"},
                }
            }
        }
        self.elastic.indices.put_mapping(index=index, doc_type='tweet', body=_mappings)

    def __fetch_tweets(self):
        try:
            conn_db_lake = db_handler.get_connection('data_lake')
            cur_db_lake = conn_db_lake.cursor()

            query = """
            SELECT * FROM tweets
            WHERE is_analyzed = FALSE
            """

            cur_db_lake.execute(query)
            return cur_db_lake.fetchall()
        except Exception as ex:
            conn_db_lake.rollback()
            raise ex


if __name__ == "__main__":
    loader = TwitterLoader()
    loader.load_into_elastic(datetime.utcnow(), **{'interval': timedelta(hours=15)})
