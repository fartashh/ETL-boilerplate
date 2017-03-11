import os
import sys
sys.path.append(os.pardir)

from celery import Celery
from services import db_connection_handler as db_handler

app = Celery('tasks',broker='amqp://localhost//')



@app.task
def reverse(string):
	return string[::-1]


@app.task
def save_into_db(type, data, created_at):
    try:
        conn_db_lake = db_handler.get_connection('data_lake')
        cur_db_lake = conn_db_lake.cursor()

        query = """INSERT INTO records (type, data, created_at) VALUES ('{}', '{}', '{}')""".format(
            type, data,created_at)
        cur_db_lake.execute(query)
        conn_db_lake.commit()
    except Exception as ex:
        conn_db_lake.rollback()
        raise ex


if __name__ == "__main__":
    save_into_db('tweet','{"a":1}',"2017/03/01")