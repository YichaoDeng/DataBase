from config import mysql_config
from db import MySqlDB, DataClient


def foo_1():
    db = MySqlDB(conf=mysql_config)

    print(db.status)


def foo_2():
    data_client = DataClient(mysql_config)
    data_client.get_db_by_name('test')


if __name__ == '__main__':
    foo_1()
