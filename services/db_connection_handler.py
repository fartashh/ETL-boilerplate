from config import app_settings
import psycopg2



def get_connection(db_name):
    try:
        db_settings = app_settings.DB_CONF[db_name]
        conn = psycopg2.connect(
            "dbname='{dbname}' user='{user}' host='{host}' password='{password}'".format(**db_settings))
        return conn
    except Exception as ex:
        raise ex

if __name__ == "__main__":
    print get_connection('data_lake')