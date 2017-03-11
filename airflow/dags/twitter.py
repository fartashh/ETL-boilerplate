import sys
sys.path.append('/home/fartash/Dev/ETL-boilerplate')

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from app.transformer.twitter import TwitterTransformer
from app.loader.twitter import TwitterLoader


default_args = {
    'owner': 'Fartash',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['fartashh@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('twitter', default_args=default_args, schedule_interval=timedelta(minutes=5))

twitter_transformer = TwitterTransformer()
twitter_loader = TwitterLoader()





analyze_tweets = PythonOperator(
    task_id='analyze_tweets',
    provide_context=True,
    python_callable=twitter_transformer.process,
    dag=dag)

transfer_to_elastic = PythonOperator(
    task_id='transfer_to_elastic',
    provide_context=True,
    python_callable=twitter_loader.load_into_elastic,
    dag=dag)

analyze_tweets.set_downstream(transfer_to_elastic)