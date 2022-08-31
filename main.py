from config import mysql_config
from db import MySqlDB

if __name__ == '__main__':
    db = MySqlDB(conf=mysql_config)

    print(db.status)
